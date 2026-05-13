package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultAddr   = ":8080"
	defaultDBPath = "nodes.json"
	checkEvery    = time.Hour
	checkTimeout  = 15 * time.Second
	failAfter     = 6 * checkEvery
	deleteAfter   = 24 * checkEvery
	historySize   = 6
	pageSize      = 50
)

var (
	indexTmpl  = template.Must(template.ParseFiles("templates/index.html"))
	httpClient = &http.Client{
		Timeout: checkTimeout,
	}
)

type App struct {
	mu     sync.RWMutex
	dbPath string
	nodes  map[string]*Node
}

type Node struct {
	ID              string    `json:"id"`
	URL             string    `json:"url"`
	Network         string    `json:"network"`
	Chain           string    `json:"chain"`
	SubmittedAt     time.Time `json:"submitted_at"`
	LastCheckedAt   time.Time `json:"last_checked_at,omitempty"`
	LastOKAt        time.Time `json:"last_ok_at,omitempty"`
	Healthy         bool      `json:"healthy"`
	Height          uint64    `json:"height,omitempty"`
	LastBlockPushed string    `json:"last_block_pushed,omitempty"`
	TotalDifficulty uint64    `json:"total_difficulty,omitempty"`
	LatencyMS       int64     `json:"latency_ms,omitempty"`
	Error           string    `json:"error,omitempty"`
	History         []bool    `json:"history,omitempty"`
}

type PageData struct {
	Nodes       []*Node
	Stats       Stats
	Filter      string
	ChainFilter string
	Message     string
	Error       string
	Pagination  Pagination
}

type Pagination struct {
	Page       int
	TotalPages int
	TotalNodes int
	Start      int
	End        int
	PrevURL    string
	NextURL    string
}

type Stats struct {
	Total     int
	Healthy   int
	Unhealthy int
	Clearnet  int
	Onion     int
	I2P       int
	Unknown   int
	Mainnet   int
	Testnet   int
}

type rpcResponse struct {
	Error any `json:"error"`
}

type tipRPCResponse struct {
	Result map[string]tipResult `json:"result"`
	Error  any                  `json:"error"`
}

type tipResult struct {
	Height          uint64 `json:"height"`
	LastBlockPushed string `json:"last_block_pushed"`
	TotalDifficulty uint64 `json:"total_difficulty"`
}

type headerRPCResponse struct {
	Result map[string]headerResult `json:"result"`
	Error  any                     `json:"error"`
}

type headerResult struct {
	Hash string `json:"hash"`
}

func main() {
	dbPath := getenv("GRIN_FAIL_DB", defaultDBPath)
	addr := listenAddr()

	app, err := NewApp(dbPath)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", app.handleIndex)
	mux.HandleFunc("/add", app.handleAdd)
	mux.HandleFunc("/nodes.json", app.handleJSON)
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	go app.checkLoop()

	log.Printf("grin.fail listening on http://%s", displayAddr(addr))
	log.Fatal(http.ListenAndServe(addr, mux))
}

func NewApp(dbPath string) (*App, error) {
	app := &App{
		dbPath: dbPath,
		nodes:  make(map[string]*Node),
	}
	if err := app.load(); err != nil {
		return nil, err
	}
	return app, nil
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	filter := strings.TrimSpace(r.URL.Query().Get("type"))
	chainFilter := strings.TrimSpace(r.URL.Query().Get("chain"))
	nodes := a.filteredNodes(filter, chainFilter)
	page := parsePage(r.URL.Query().Get("page"))
	pageNodes, pagination := paginateNodes(nodes, page, r.URL.Query())
	data := PageData{
		Nodes:       pageNodes,
		Stats:       stats(nodes),
		Filter:      filter,
		ChainFilter: chainFilter,
		Message:     r.URL.Query().Get("ok"),
		Error:       r.URL.Query().Get("err"),
		Pagination:  pagination,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := indexTmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *App) handleAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	raw := strings.TrimSpace(r.FormValue("node_url"))
	nodeURL, err := normalizeURL(raw)
	if err != nil {
		redirectHome(w, r, "", err.Error())
		return
	}

	node := &Node{
		ID:          nodeID(nodeURL),
		URL:         nodeURL,
		Network:     detectNetwork(nodeURL),
		SubmittedAt: time.Now().UTC(),
	}

	a.mu.Lock()
	existing := a.nodes[node.ID]
	if existing == nil {
		a.nodes[node.ID] = node
	}
	a.mu.Unlock()

	if existing != nil {
		redirectHome(w, r, "node already exists", "")
		return
	}

	if err := a.checkNode(node.ID); err != nil {
		a.mu.Lock()
		delete(a.nodes, node.ID)
		a.mu.Unlock()
		redirectHome(w, r, "", "node did not answer Grin foreign API checks")
		return
	}
	if err := a.save(); err != nil {
		log.Printf("save failed: %v", err)
	}
	redirectHome(w, r, "node submitted", "")
}

func (a *App) handleJSON(w http.ResponseWriter, r *http.Request) {
	nodes := a.allNodes()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(nodes)
}

func (a *App) checkLoop() {
	a.checkDue()
	ticker := time.NewTicker(checkEvery)
	defer ticker.Stop()
	for range ticker.C {
		a.checkDue()
	}
}

func (a *App) checkDue() {
	now := time.Now().UTC()
	a.mu.RLock()
	ids := make([]string, 0, len(a.nodes))
	for id, node := range a.nodes {
		if !node.LastCheckedAt.IsZero() && now.Sub(node.LastCheckedAt) < checkEvery {
			continue
		}
		ids = append(ids, id)
	}
	a.mu.RUnlock()
	sort.Strings(ids)

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := a.checkNode(id); err != nil {
				log.Printf("check failed: %v", err)
			}
		}(id)
	}
	wg.Wait()
	a.pruneDeadNodes()
	if err := a.save(); err != nil {
		log.Printf("save failed: %v", err)
	}
}

func (a *App) checkNode(id string) error {
	a.mu.RLock()
	node := a.nodes[id]
	a.mu.RUnlock()
	if node == nil {
		return errors.New("node not found")
	}

	start := time.Now()
	endpoint := foreignEndpoint(node.URL)
	tip, err := fetchTip(endpoint)
	latency := time.Since(start).Milliseconds()

	a.mu.Lock()
	defer a.mu.Unlock()
	node.LastCheckedAt = time.Now().UTC()
	node.LatencyMS = latency
	if err != nil {
		node.Healthy = nodeStillHealthy(node, node.LastCheckedAt)
		node.Error = err.Error()
		node.History = appendHistory(node.History, false)
		return err
	}
	if node.Chain == "" {
		chain, err := detectChain(endpoint)
		if err != nil {
			node.Healthy = nodeStillHealthy(node, node.LastCheckedAt)
			node.Error = err.Error()
			node.History = appendHistory(node.History, false)
			return err
		}
		node.Chain = chain
	}
	node.Healthy = true
	node.Error = ""
	node.LastOKAt = node.LastCheckedAt
	node.Height = tip.Height
	node.LastBlockPushed = tip.LastBlockPushed
	node.TotalDifficulty = tip.TotalDifficulty
	node.History = appendHistory(node.History, true)
	return nil
}

func nodeStillHealthy(node *Node, now time.Time) bool {
	if node.LastOKAt.IsZero() {
		return false
	}
	return now.Sub(node.LastOKAt) < failAfter
}

func (a *App) pruneDeadNodes() {
	now := time.Now().UTC()
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, node := range a.nodes {
		if node.LastOKAt.IsZero() || now.Sub(node.LastOKAt) >= deleteAfter {
			delete(a.nodes, id)
		}
	}
}

func fetchTip(endpoint string) (tipResult, error) {
	var rpc tipRPCResponse
	if err := callForeign(endpoint, []byte(`{"jsonrpc":"2.0","method":"get_tip","params":[],"id":1}`), &rpc); err != nil {
		return tipResult{}, err
	}
	tip, ok := rpc.Result["Ok"]
	if !ok {
		return tipResult{}, errors.New("missing result.Ok")
	}
	return tip, nil
}

func detectChain(endpoint string) (string, error) {
	var rpc headerRPCResponse
	if err := callForeign(endpoint, []byte(`{"jsonrpc":"2.0","method":"get_header","params":[0,null,null],"id":1}`), &rpc); err != nil {
		return "", err
	}
	header, ok := rpc.Result["Ok"]
	if !ok {
		return "", errors.New("missing genesis header")
	}
	return chainFromGenesisHash(header.Hash)
}

func chainFromGenesisHash(hash string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(hash)) {
	case "40adad0aec27797b48840aa9e00472015c21baea118ce7a2ff1a82c0f8f5bf82":
		return "mainnet", nil
	case "edc758c1370d43e1d733f70f58cf187c3be8242830429b1676b89fd91ccf2dab":
		return "testnet", nil
	default:
		return "", fmt.Errorf("unknown genesis hash %s", hash)
	}
}

func callForeign(endpoint string, payload []byte, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "grin.fail node checker")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if err := json.Unmarshal(body, out); err != nil {
		return err
	}
	var base rpcResponse
	if err := json.Unmarshal(body, &base); err != nil {
		return err
	}
	if base.Error != nil {
		return fmt.Errorf("json-rpc error: %v", base.Error)
	}
	return nil
}

func normalizeURL(raw string) (string, error) {
	if raw == "" {
		return "", errors.New("node URL is required")
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("invalid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("only http and https URLs are supported")
	}
	if u.Host == "" {
		return "", errors.New("URL must include a host")
	}
	u.Fragment = ""
	u.RawQuery = ""
	u.Path = strings.TrimRight(u.EscapedPath(), "/")
	if u.Path == "" {
		u.Path = ""
	}
	return u.String(), nil
}

func foreignEndpoint(nodeURL string) string {
	u, err := url.Parse(nodeURL)
	if err != nil {
		return nodeURL
	}
	check := *u
	if check.Path == "" || check.Path == "/" {
		check.Path = "/v2/foreign"
	}
	return check.String()
}

func detectNetwork(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "unknown"
	}
	host := strings.ToLower(u.Hostname())
	switch {
	case strings.HasSuffix(host, ".onion"):
		return "onion"
	case strings.HasSuffix(host, ".i2p"), strings.HasSuffix(host, ".b32.i2p"):
		return "i2p"
	case host == "":
		return "unknown"
	default:
		return "clearnet"
	}
}

func (a *App) filteredNodes(filter, chainFilter string) []*Node {
	a.mu.RLock()
	defer a.mu.RUnlock()
	nodes := make([]*Node, 0, len(a.nodes))
	now := time.Now().UTC()
	for _, node := range a.nodes {
		chain := node.Chain
		if chain == "" {
			chain = "unknown"
		}
		if !nodeStillHealthy(node, now) {
			continue
		}
		if filter != "" && filter != "all" && node.Network != filter {
			continue
		}
		if chainFilter != "" && chainFilter != "all" && chain != chainFilter {
			continue
		}
		copyNode := *node
		copyNode.Chain = chain
		copyNode.History = append([]bool(nil), node.History...)
		nodes = append(nodes, &copyNode)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if !nodes[i].SubmittedAt.Equal(nodes[j].SubmittedAt) {
			return nodes[i].SubmittedAt.After(nodes[j].SubmittedAt)
		}
		return nodes[i].URL < nodes[j].URL
	})
	return nodes
}

func (a *App) allNodes() []*Node {
	a.mu.RLock()
	defer a.mu.RUnlock()
	nodes := make([]*Node, 0, len(a.nodes))
	for _, node := range a.nodes {
		copyNode := *node
		copyNode.History = append([]bool(nil), node.History...)
		nodes = append(nodes, &copyNode)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if !nodes[i].SubmittedAt.Equal(nodes[j].SubmittedAt) {
			return nodes[i].SubmittedAt.After(nodes[j].SubmittedAt)
		}
		return nodes[i].URL < nodes[j].URL
	})
	return nodes
}

func parsePage(raw string) int {
	page, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || page < 1 {
		return 1
	}
	return page
}

func paginateNodes(nodes []*Node, page int, query url.Values) ([]*Node, Pagination) {
	total := len(nodes)
	totalPages := 1
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}

	p := Pagination{
		Page:       page,
		TotalPages: totalPages,
		TotalNodes: total,
		Start:      start + 1,
		End:        end,
	}
	if total == 0 {
		p.Start = 0
	}
	if page > 1 {
		p.PrevURL = pageURL(query, page-1)
	}
	if page < totalPages {
		p.NextURL = pageURL(query, page+1)
	}
	return nodes[start:end], p
}

func pageURL(query url.Values, page int) string {
	q := url.Values{}
	for key, values := range query {
		if key == "ok" || key == "err" {
			continue
		}
		for _, value := range values {
			q.Add(key, value)
		}
	}
	if page <= 1 {
		q.Del("page")
	} else {
		q.Set("page", strconv.Itoa(page))
	}
	if encoded := q.Encode(); encoded != "" {
		return "/?" + encoded + "#findnodes"
	}
	return "/#findnodes"
}

func stats(nodes []*Node) Stats {
	var s Stats
	for _, n := range nodes {
		if !everSucceeded(n) {
			continue
		}
		s.Total++
		if n.Healthy {
			s.Healthy++
		} else {
			s.Unhealthy++
		}
		switch n.Network {
		case "clearnet":
			s.Clearnet++
		case "onion":
			s.Onion++
		case "i2p":
			s.I2P++
		default:
			s.Unknown++
		}
		switch n.Chain {
		case "mainnet":
			s.Mainnet++
		case "testnet":
			s.Testnet++
		}
	}
	return s
}

func everSucceeded(n *Node) bool {
	if !n.LastOKAt.IsZero() {
		return true
	}
	return n.Healthy && n.Height > 0
}

func nodeID(s string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(s)))
	return hex.EncodeToString(sum[:8])
}

func appendHistory(history []bool, ok bool) []bool {
	history = append(history, ok)
	if len(history) > historySize {
		history = history[len(history)-historySize:]
	}
	return history
}

func (a *App) load() error {
	f, err := os.Open(a.dbPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	var nodes []*Node
	if err := json.NewDecoder(f).Decode(&nodes); err != nil {
		return err
	}
	for _, n := range nodes {
		if n.URL == "" {
			continue
		}
		if normalized, err := normalizeURL(n.URL); err == nil {
			n.URL = normalized
		}
		if n.ID == "" {
			n.ID = nodeID(n.URL)
		}
		if n.Chain == "" {
			n.Chain = "unknown"
		}
		if len(n.History) > historySize {
			n.History = n.History[len(n.History)-historySize:]
		}
		n.Healthy = nodeStillHealthy(n, time.Now().UTC())
		a.nodes[n.ID] = n
	}
	return nil
}

func (a *App) save() error {
	a.mu.RLock()
	nodes := make([]*Node, 0, len(a.nodes))
	for _, node := range a.nodes {
		copyNode := *node
		copyNode.History = append([]bool(nil), node.History...)
		nodes = append(nodes, &copyNode)
	}
	a.mu.RUnlock()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].URL < nodes[j].URL })

	tmp := a.dbPath + ".tmp"
	if dir := path.Dir(a.dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(nodes); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, a.dbPath)
}

func redirectHome(w http.ResponseWriter, r *http.Request, okMsg, errMsg string) {
	q := url.Values{}
	if okMsg != "" {
		q.Set("ok", okMsg)
	}
	if errMsg != "" {
		q.Set("err", errMsg)
	}
	target := "/"
	if encoded := q.Encode(); encoded != "" {
		target += "?" + encoded
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func getenv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func listenAddr() string {
	if len(os.Args) > 1 && strings.TrimSpace(os.Args[1]) != "" {
		return strings.TrimSpace(os.Args[1])
	}
	return getenv("ADDR", defaultAddr)
}

func displayAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}
