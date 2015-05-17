package main

import (
    "bytes"
    "database/sql"
    "encoding/json"
    "encoding/csv"
    "fmt"
    "html/template"
    "log"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    _ "github.com/go-sql-driver/mysql"
    "github.com/microcosm-cc/bluemonday"
)

type Configuration struct {
    Dsn   string
    Esurl string
    Esurl2 string
    Sql1   string
    Sql2   string
    Size  int
    Datakey string
    Valuekey string
    Yappid string
}

type Event struct {
    EventId  string
    Name     string
    Subtitle string
    UserId   string
    UserName string
    Datetime string
    VenueName string
    Address string
    SeatsSold string
    SeatsMax string
}

type SearchResult struct {
    Events []Event
    Total int
    Keyword string
    EventId string
    Page int
    Mtf int
    Xdf int
    Xqt int
}

type WordsResult struct {
    Word string
    Weight float64
}

type KeywordsResult struct {
    Words []WordsResult
    Eid string
}

var templates = template.Must(template.ParseFiles("search-keyword.html", "search-similar.html", "keywords.html"))
var config Configuration

func readConfig() error {
    config = Configuration{}
    file, err := os.Open("conf.json")
    if err != nil {
        return err
    }

    decoder := json.NewDecoder(file)
    err = decoder.Decode(&config)
    if err != nil {
        return err;
    }

    return nil;
}

func getEvents(eids []string) ([]Event, int, error) {
    var events []Event

    if len(eids) <= 0 {
        return events, 0, nil
    }

    db, err := sql.Open("mysql", config.Dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    sqlWhere := "?" + strings.Repeat(",?", len(eids)-1)
    sql := fmt.Sprintf(config.Sql1, sqlWhere)

    stmt, err := db.Prepare(sql)
    if err != nil {
        log.Fatal(err)
    }

    var args []interface{}
    args = make([]interface{}, len(eids))
    for i, v := range eids {
        args[i] = v
    }

    rows, err := stmt.Query(args...)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    var totalSeatsSold int
    event := Event{}
    for rows.Next() {
        err := rows.Scan(
            &event.EventId,
            &event.Name,
            &event.Subtitle,
            &event.UserId,
            &event.UserName,
            &event.Datetime,
            &event.VenueName,
            &event.Address,
            &event.SeatsSold,
            &event.SeatsMax)
        if err != nil {
                log.Fatal(err)
        }
        events = append(events, event)
        seatsSoldInt, _ := strconv.Atoi(event.SeatsSold)
        totalSeatsSold += seatsSoldInt
    }
    err = rows.Err()
    if err != nil {
        log.Fatal(err)
    }

    return events, totalSeatsSold, nil
}

func searchEventsByKeyword(keyword string, page int) ([]string, error) {
    encodedKeyword := url.QueryEscape(keyword)
    url := fmt.Sprintf(config.Esurl, strconv.Itoa(page), encodedKeyword)
    page -= 1
    if page < 0 {
        page = 0
    }
    page = config.Size * page

    resp, err := http.Get(url)
    if err != nil {
        log.Fatal(err)
        return nil, nil
    }
    defer resp.Body.Close()

    var root map[string]interface{}
    decoder := json.NewDecoder(resp.Body)
    err = decoder.Decode(&root)
    if err != nil {
        log.Fatal(err)
        return nil, nil
    }

    _data, _ := root[config.Datakey].(map[string]interface{})
    data, _ := _data[config.Datakey].([]interface{})
    eids := make([]string, len(data))
    for i, _row := range data {
        row, _ := _row.(map[string]interface{})
        eids[i] = row[config.Valuekey].(string)
    }
    return eids, nil
}

func searchSimilarEventsByEventId(eventId string, page int, mtf int, xdf int, xqt int) ([]string, error) {
    mlt := make(map[string]interface {})
    fields := [2]string{"name", "description"}
    mlt["fields"] = fields

    doc := make(map[string]string)
    doc["_index"] = "event2"
    doc["_type"] = "default"
    doc["_id"] = eventId
    docs := make([]map[string]string, 1)
    docs[0] = doc
    mlt["docs"] = docs
    mlt["min_term_freq"] = mtf
    mlt["max_doc_freq"] = xdf
    mlt["max_query_terms"] = xqt

    query := make(map[string]interface{})
    query["more_like_this"] = mlt

    page -= 1
    if page < 0 {
        page = 0
    }
    page = config.Size * page

    q := make(map[string]interface{})
    q["from"] = strconv.Itoa(page)
    q["size"] = "100"
    q["query"] = query

    buf := new(bytes.Buffer)
    encoder := json.NewEncoder(buf)
    err := encoder.Encode(q)
    if err != nil {
        return nil, err;
    }

    jsonstr := buf.String()

    client := &http.Client{}
    r, _ := http.NewRequest("POST", config.Esurl2, bytes.NewBufferString(jsonstr))

    r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
    r.Header.Add("Content-Length", strconv.Itoa(len(jsonstr)))

    resp, err := client.Do(r)
    if err != nil {
        log.Fatal(err)
        return nil, err
    }
    defer resp.Body.Close()

    var root map[string]interface{}
    decoder := json.NewDecoder(resp.Body)
    err = decoder.Decode(&root)
    if err != nil {
        log.Fatal(err)
        return nil, nil
    }

    _data, _ := root[config.Datakey].(map[string]interface{})
    data, _ := _data[config.Datakey].([]interface{})
    eids := make([]string, len(data))
    for i, _row := range data {
        row, _ := _row.(map[string]interface{})
        eids[i] = row[config.Valuekey].(string)
    }
    return eids, nil
}

func similarSearchHandler(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    eventIds := query["q"]
    pages := query["p"]
    mtfs := query["mtf"]
    xdfs := query["xdf"]
    xqts := query["xqt"]

    if _, ok := query["csv"]; ok {
        similarSearchCsvHandler(w, r);
        return;
    }

    result := new(SearchResult)

    var page int
    if len(pages) > 0 {
        page, _ = strconv.Atoi(pages[0])
    } else {
        page = 1
    }

    result.Page = page

    var mtf int
    var xdf int
    var xqt int
    if (len(mtfs) > 0) {
        mtf, _ = strconv.Atoi(mtfs[0])
    } else {
        mtf = 1
    }
    result.Mtf = mtf
    if (len(xdfs) > 0) {
        xdf, _ = strconv.Atoi(xdfs[0])
    } else {
        xdf = 10000
    }
    result.Xdf = xdf
    if (len(xqts) > 0) {
        xqt, _ = strconv.Atoi(xqts[0])
    } else {
        xqt = 25
    }
    result.Xqt = xqt

    if len(eventIds) <= 0 {
        err := templates.ExecuteTemplate(w, "search-similar.html", result)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
        return;
    }

    eventId := eventIds[0]
    result.EventId = eventId

    eids, err := searchSimilarEventsByEventId(eventId, page, mtf, xdf, xqt)
    events, total, err := getEvents(eids)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    result.Events = events
    result.Total = total

    err = templates.ExecuteTemplate(w, "search-similar.html", result)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
}

func keywordSearchHandler(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    keywords := query["q"]
    pages := query["p"]

    if _, ok := query["csv"]; ok {
        keywordSearchCsvHandler(w, r);
        return;
    }

    result := new(SearchResult)

    var page int
    if len(pages) > 0 {
        page, _ = strconv.Atoi(pages[0])
    } else {
        page = 1
    }
    result.Page = page
    if len(keywords) <= 0 {
        err := templates.ExecuteTemplate(w, "search-keyword.html", result)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
        return;
    }

    keyword := keywords[0]

    result.Keyword = keyword

    eids, err := searchEventsByKeyword(keyword, page)
    events, total, err := getEvents(eids)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    result.Events = events
    result.Total = total

    err = templates.ExecuteTemplate(w, "search-keyword.html", result)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
}

func similarSearchCsvHandler(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    eventIds := query["q"]
    pages := query["p"]
    mtfs := query["mtf"]
    xdfs := query["xdf"]
    xqts := query["xqt"]

    w.Header().Set("Content-Type", "text/csv")
    w.Header().Set("Content-disposition", "attachment; filename=" + url.QueryEscape(eventIds[0]) + "_" + pages[0] + ".csv")

    result := new(SearchResult)
    if len(eventIds) <= 0 {
        http.Redirect(w, r, "/search/similar", http.StatusFound)
        return;
    }

    eventId := eventIds[0]

    var page int
    if len(pages) > 0 {
        page, _ = strconv.Atoi(pages[0])
    } else {
        page = 1
    }

    result.EventId = eventId
    result.Page = page

    var mtf int
    var xdf int
    var xqt int
    if (len(mtfs) > 0) {
        mtf, _ = strconv.Atoi(mtfs[0])
    } else {
        mtf = 1
    }
    if (len(xdfs) > 0) {
        xdf, _ = strconv.Atoi(xdfs[0])
    } else {
        xdf = 1
    }
    if (len(xqts) > 0) {
        xqt, _ = strconv.Atoi(xqts[0])
    } else {
        xqt = 1
    }
    eids, err := searchSimilarEventsByEventId(eventId, page, mtf, xdf, xqt)
    events, total, err := getEvents(eids)
    if err != nil {
        http.Redirect(w, r, "/search/similar", http.StatusFound)
        return
    }

    result.Events = events
    result.Total = total

    csvWriter := csv.NewWriter(w)

    for _, e := range events {
        _ = csvWriter.Write([]string { e.EventId, e.Name, e.Subtitle, e.UserId, e.UserName, e.Datetime, e.VenueName, e.Address, e.SeatsSold })
    }
    csvWriter.Flush()
}

func keywordSearchCsvHandler(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    keywords := query["q"]
    pages := query["p"]

    w.Header().Set("Content-Type", "text/csv")
    w.Header().Set("Content-disposition", "attachment; filename=" + url.QueryEscape(keywords[0]) + "_" + pages[0] + ".csv")

    result := new(SearchResult)
    if len(keywords) <= 0 {
        http.Redirect(w, r, "/search/keyword", http.StatusFound)
        return;
    }

    keyword := keywords[0]

    var page int
    if len(pages) > 0 {
        page, _ = strconv.Atoi(pages[0])
    } else {
        page = 1
    }

    result.Keyword = keyword
    result.Page = page

    eids, err := searchEventsByKeyword(keyword, page)
    events, total, err := getEvents(eids)
    if err != nil {
        http.Redirect(w, r, "/search/keyword", http.StatusFound)
        return
    }

    result.Events = events
    result.Total = total

    csvWriter := csv.NewWriter(w)

    for _, e := range events {
        _ = csvWriter.Write([]string { e.EventId, e.Name, e.Subtitle, e.UserId, e.UserName, e.Datetime, e.VenueName, e.Address, e.SeatsSold })
    }
    csvWriter.Flush()
}

func getDescriptiveStrings(eid string) (string, error) {
    db, err := sql.Open("mysql", config.Dsn)
    if err != nil {
        log.Fatal(err)
        return "", err
    }
    defer db.Close()

    row := db.QueryRow(config.Sql2, eid)

    var (
        _name sql.NullString
        _venue_name sql.NullString
        _description sql.NullString
        _subtitle sql.NullString
        name string
        venue_name string
        description string
        subtitle string
    )

    err = row.Scan(&_name, &_venue_name, &_description, &_subtitle)
    if err == sql.ErrNoRows {
        log.Println("No rows " + eid)
        return "", err
    }
    if err != nil {
        log.Fatal(err)
        return "", err
    }
    if _name.Valid {
        name = _name.String
    }
    if _venue_name.Valid {
        venue_name = _venue_name.String
    }
    if _description.Valid {
        description = _description.String
    }
    if _subtitle.Valid {
        subtitle = _subtitle.String
    }

    sanitizer := bluemonday.StrictPolicy()
    description = sanitizer.Sanitize(description)

    return fmt.Sprintf("%s %s %s %s", name, subtitle, description, venue_name), nil
}

func collectKeywords(eid string) ([]WordsResult, error) {
    var results []WordsResult
    words, err := getDescriptiveStrings(eid)
    if err == sql.ErrNoRows {
        return results, nil
    }
    if err != nil {
        log.Fatal(err)
        return nil, err
    }

    data := url.Values{}
    data.Set("appid", config.Yappid)
    data.Set("output", "json")
    data.Set("sentence", words)

    client := &http.Client{}
    r, _ := http.NewRequest("POST", "http://jlp.yahooapis.jp/KeyphraseService/V1/extract", bytes.NewBufferString(data.Encode()))
    r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
    r.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

    resp, err := client.Do(r)
    if err != nil {
        log.Fatal(err)
        return nil, err
    }
    defer resp.Body.Close()

    var root map[string]interface{}
    decoder := json.NewDecoder(resp.Body)
    err = decoder.Decode(&root)
    if err != nil {
        log.Fatal(err)
        return nil, err
    }

    //fmt.Printf("%#v", root)

    for k, v := range root {
        result := WordsResult{Word:k, Weight:v.(float64)}
        results = append(results, result)
    }

    return results, nil
}

func keywordsHandler(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    eids := query["eid"]

    result := new(KeywordsResult)
    if len(eids) <= 0 {
        err := templates.ExecuteTemplate(w, "keywords.html", result)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
        return;
    }

    eid := eids[0]
    result.Eid = eid

    keywords, err := collectKeywords(eid)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    result.Words = keywords

    err = templates.ExecuteTemplate(w, "keywords.html", result)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
}

func main() {
    err := readConfig()
    if err != nil {
        log.Fatal(err)
    }

    //var page int
    //flag.IntVar(&page, "page", 1, "number of page to search for (starts 1)")
    //var eid int
    //flag.IntVar(&eid, "id", 0, "event id")

    //flag.Parse()

    //if len(flag.Args()) <= 0 {
    //    log.Fatal("keyword required")
    //}
    //keyword := flag.Args()[0]

    http.HandleFunc("/search/keyword/", keywordSearchHandler)
    http.HandleFunc("/search/similar/", similarSearchHandler)
    http.HandleFunc("/keywords/", keywordsHandler)
    http.ListenAndServe(":8080", nil)
}
