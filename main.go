package main

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

const (
	interval         = time.Hour
	articleThreshold = time.Hour * 24 * 3
)

type Rss struct {
	XMLName xml.Name `xml:"rss"`
	Channel struct {
		Title       string `xml:"title"`
		Link        string `xml:"link"`
		Description string `xml:"description"`
		Items       []struct {
			Title       string `xml:"title"`
			Link        string `xml:"link"`
			Description string `xml:"description"`
			PubDate     string `xml:"pubDate"`
		} `xml:"item"`
	} `xml:"channel"`
}

type ESPayload struct {
	Title       string `json:"title"`
	Description string `json:"description"`
}

type ESUpdatePayload struct {
	Doc struct {
		Title       string `json:"title"`
		Description string `json:"description"`
	} `json:"doc"`
}

type ArticleIdAndPubDate struct {
	id      string
	pubDate sql.NullTime
}

func initDB() *sql.DB {
	dbUrl := os.Getenv("DB_URL")

	if dbUrl == "" {
		log.Fatal("DB_URL is not specified")
	}

	db, err := sql.Open("postgres", dbUrl)

	if err != nil {
		log.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	return db
}

func initElasticsearch() *elasticsearch.Client {
	esClient, err := elasticsearch.NewDefaultClient()

	if err != nil {
		log.Fatal(err)
	}

	_, err = esClient.Info()

	if err != nil {
		log.Fatal(err)
	}

	return esClient
}

func scrapeRss(feedUrl string) Rss {
	res, err := http.Get(feedUrl)
	if err != nil {
		log.Fatal(fmt.Sprintf("HTTP request error (%s): %s", feedUrl, err))
	}

	defer res.Body.Close()

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal("Response body reading error:", err)
	}

	var parsedData Rss
	err = xml.Unmarshal(resBody, &parsedData)
	if err != nil {
		log.Fatal(fmt.Sprintf("XML parsing error on %v ", feedUrl), err)
	}

	return parsedData
}

func pushArticles(db *sql.DB, esClient *elasticsearch.Client, siteId string, data Rss) {
	for _, article := range data.Channel.Items {
		if article.Title == "" || article.Link == "" || article.Description == "" {
			log.Println("> > Empty string encountered. Aborting.")
			break
		}

		articleId := base64.URLEncoding.EncodeToString([]byte(article.Link))

		title := strings.ReplaceAll(article.Title, "'", "`")
		description := strings.ReplaceAll(article.Description, "'", "`")

		err := pushArticleToDB(
			db,
			articleId,
			title,
			strings.ReplaceAll(article.Link, "'", "`"),
			description,
			strings.ReplaceAll(article.PubDate, "'", "`"),
			siteId,
		)

		if err != nil {
			log.Println(err)
			return
		}

		err = pushArticleToES(esClient, articleId, title, description)

		if err != nil {
			log.Println(err)
			return
		}
	}
}

func pushArticleToDB(db *sql.DB, articleId, title, link, descrtiption, pubDate, siteId string) error {
	_, err := db.Exec(
		`INSERT INTO Articles VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (id) DO UPDATE SET 
        title = EXCLUDED.title,
        description = EXCLUDED.description
    `,
		articleId,
		title,
		link,
		descrtiption,
		pubDate,
		siteId,
	)
	return err
}

func pushArticleToES(esClient *elasticsearch.Client, articleId, title, description string) error {
	payload := ESUpdatePayload{
		Doc: ESPayload{
			Title:       title,
			Description: description,
		},
	}
	data, err := json.Marshal(payload)

	if err != nil {
		return err
	}

	res, err := esClient.Update("articles", articleId, bytes.NewReader(data))

	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			createPayload := ESPayload{
				Title:       payload.Doc.Title,
				Description: payload.Doc.Description,
			}
			createData, err := json.Marshal(createPayload)
			if err != nil {
				return nil
			}
			res, err = esClient.Create("articles", articleId, bytes.NewReader(createData))
			if res.IsError() {
				errBody, err := io.ReadAll(res.Body)
				if err == nil {
					return fmt.Errorf("ES error: %s :: %s", res.Status(), string(errBody))
				}
			}
		}
	}

	return err
}

func pushSite(db *sql.DB, esClient *elasticsearch.Client, feedUrl string, data Rss) (string, error) {
	if data.Channel.Title == "" || data.Channel.Link == "" {
		return "", fmt.Errorf("> Empty string encountered. Aborting.")
	}

	title := strings.ReplaceAll(data.Channel.Title, "'", "`")
	description := strings.ReplaceAll(data.Channel.Description, "'", "`")

	siteId := base64.URLEncoding.EncodeToString([]byte(feedUrl))

	err := pushSiteToDB(
		db,
		siteId,
		feedUrl,
		title,
		strings.ReplaceAll(data.Channel.Link, "'", "`"),
		description,
	)

	if err != nil {
		return feedUrl, err
	}

	err = pushSiteToES(
		esClient,
		siteId,
		title,
		description,
	)

	return siteId, err
}

func pushSiteToDB(db *sql.DB, siteId, feedUrl, title, link, description string) error {

	_, err := db.Exec(
		`INSERT INTO Sites VALUES ($1, $2, $3, $4, $5) 
    ON CONFLICT (id) DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description
    `,
		siteId,
		feedUrl,
		title,
		link,
		description,
	)

	return err
}

func pushSiteToES(esClient *elasticsearch.Client, siteId, title, description string) error {
	payload := ESUpdatePayload{
		Doc: ESPayload{
			Title:       title,
			Description: description,
		},
	}
	data, err := json.Marshal(payload)

	if err != nil {
		return err
	}

	res, err := esClient.Update("sites", siteId, bytes.NewReader(data))

	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			createPayload := ESPayload{
				Title:       payload.Doc.Title,
				Description: payload.Doc.Description,
			}
			createData, err := json.Marshal(createPayload)
			if err != nil {
				return nil
			}
			res, err = esClient.Create("sites", siteId, bytes.NewReader(createData))
			if res.IsError() {
				errBody, err := io.ReadAll(res.Body)
				if err == nil {
					return fmt.Errorf("ES error: %s :: %s", res.Status(), string(errBody))
				}
			}
		}
	}

	return err
}

func cleanOutdated(db *sql.DB, esClient *elasticsearch.Client) {
	log.Println("Cleanup...")

	var articleInfo ArticleIdAndPubDate
	rows, err := db.Query("SELECT id, pubdate from Articles")

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var scannedCount, deleteCount int
	for rows.Next() {
		if err := rows.Scan(&articleInfo.id, &articleInfo.pubDate); err != nil {
			log.Fatal(err)
		}
		if time.Now().Sub(articleInfo.pubDate.Time) > articleThreshold {
			_, err = db.Exec("DELETE FROM Articles WHERE id = $1", articleInfo.id)
			if err != nil {
				log.Fatal(err)
			}

			res, err := esClient.Delete("articles", articleInfo.id)

			if err != nil {
				log.Fatal(err)
			}

			if res.IsError() {
				resBody, err := io.ReadAll(res.Body)
				if err != nil {
					log.Fatal(err)
				}
				log.Fatal("ES error:", resBody)
			}

			deleteCount += 1
		}
		scannedCount += 1
	}

	log.Printf("> Done. %d rows scanned. %d rows deleted", scannedCount, deleteCount)
}

func scrapingCycle(db *sql.DB, esClient *elasticsearch.Client, feeds []string) {
	log.Println("Scraping data...")

	var wg sync.WaitGroup
	wg.Add(len(feeds))

	for _, url := range feeds {
		go func(url string) {
			res := scrapeRss(url)
			siteId, err := pushSite(db, esClient, url, res)

			if err != nil {
				log.Println(err)
				wg.Done()
				return
			}

			pushArticles(db, esClient, siteId, res)
			wg.Done()
			return
		}(url)
	}
	wg.Wait()
}

func main() {
	godotenv.Load(".env")

	data, err := os.ReadFile("feeds.txt")
	if err != nil {
		log.Fatal(err)
	}

	feeds := strings.Split(string(data), "\n")
	feeds = feeds[:len(feeds)-1]

	db := initDB()

	defer func() {
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	esClient := initElasticsearch()

	ticker := time.NewTicker(interval)

	for {
		scrapingCycle(db, esClient, feeds)
		cleanOutdated(db, esClient)

		log.Println("Cycle over. Waiting for the next one...")
		<-ticker.C
	}
}
