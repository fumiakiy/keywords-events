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
    Page int
}

type WordsResult struct {
    Word string
    Weight float64
}

type KeywordsResult struct {
    Words []WordsResult
    Eid string
}

var templates = template.Must(template.ParseFiles("search.html", "keywords.html"))
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

func searchEvents(keyword string, page int) ([]string, error) {
    page -= 1
    if page < 0 {
        page = 0
    }
    page = config.Size * page
    encodedKeyword := url.QueryEscape(keyword)

    resp, err := http.Get(fmt.Sprintf(config.Esurl, strconv.Itoa(page), encodedKeyword))
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

func searchHandler(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    keywords := query["q"]
    pages := query["p"]

    if _, ok := query["csv"]; ok {
        csvHandler(w, r);
        return;
    }

    result := new(SearchResult)
    if len(keywords) <= 0 {
        err := templates.ExecuteTemplate(w, "search.html", result)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
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

    eids, err := searchEvents(keyword, page)
    events, total, err := getEvents(eids)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    result.Events = events
    result.Total = total

    err = templates.ExecuteTemplate(w, "search.html", result)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
}

func csvHandler(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    keywords := query["q"]
    pages := query["p"]

    w.Header().Set("Content-Type", "text/csv")
    w.Header().Set("Content-disposition", "attachment; filename=" + url.QueryEscape(keywords[0]) + "_" + pages[0] + ".csv")

    result := new(SearchResult)
    if len(keywords) <= 0 {
        http.Redirect(w, r, "/search/", http.StatusFound)
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

    eids, err := searchEvents(keyword, page)
    events, total, err := getEvents(eids)
    if err != nil {
        http.Redirect(w, r, "/search/", http.StatusFound)
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

    http.HandleFunc("/search/", searchHandler)
    http.HandleFunc("/keywords/", keywordsHandler)
    http.ListenAndServe(":8080", nil)
}
