package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// -------------------- MongoDB --------------------
var ordersCollection *mongo.Collection

// -------------------- Cache --------------------
var cache = sync.Map{}

// key: filter hash, value: report
func GetFromCache(key string) (any, bool) {
	val, ok := cache.Load(key)
	return val, ok
}

func SaveToCache(key string, data any) {
	cache.Store(key, data)
}

// -------------------- Structs --------------------
type ReportFilter struct {
	ClientIDs        []string `json:"client_ids"`
	StrategyNames    []string `json:"strategy_names"`
	Symbols          []string `json:"symbols"`
	DateFrom         string   `json:"date_from"`
	DateTo           string   `json:"date_to"`
	MinQuantity      int      `json:"min_quantity"`
	MaxQuantity      int      `json:"max_quantity"`
	MinPrice         float64  `json:"min_price"`
	MaxPrice         float64  `json:"max_price"`
	MinPnL           float64  `json:"min_pnl"`
	MaxPnL           float64  `json:"max_pnl"`
	OrderType        string   `json:"order_type"`
	IncludePositions bool     `json:"include_positions"`
	IncludeTrades    bool     `json:"include_trades"`
	Page             int      `json:"page"`
	Limit            int      `json:"limit"`
	SortField        string   `json:"sort_field"`
	SortDirection    int      `json:"sort_direction"` // 1: asc, -1: desc
}

type Job struct {
	ClientID string
	Filter   ReportFilter
}

type Result struct {
	ClientID string `json:"client_id"`
	Data     any    `json:"data"`
}

// -------------------- Pipeline Builder --------------------
func BuildPipeline(filter ReportFilter) []bson.M {

	pipeline := []bson.M{}

	// match stage
	match := matchStage(filter)
	pipeline = append(pipeline, bson.M{"$match": match})

	// lookup stage
	lookupStage := lookupStage()
	pipeline = append(pipeline, lookupStage...)

	// unwind stage
	unwindStage := unwindStage(filter)
	pipeline = append(pipeline, unwindStage...)

	//groupby stage
	groupbyStage := groupbyStage()
	pipeline = append(pipeline, groupbyStage)

	// projection stage
	projectionStage := projectionStage()
	pipeline = append(pipeline, projectionStage)

	// fmt.Println("MongoDB Aggregation Pipeline:")
	// for i, stage := range pipeline {
	// 	fmt.Printf("Stage %d: %v\n", i+1, stage)
	// }

	return pipeline
}

func projectionStage() bson.M {
	projectionStage := bson.M{"$project": bson.M{
		"client_id":     "$_id.client_id",
		"strategy_name": "$_id.strategy_name",
		"symbol":        "$_id.symbol",
		"summary": bson.M{
			"total_buy":    "$total_buy",
			"total_sell":   "$total_sell",
			"net_quantity": "$net_quantity",
			"turnover":     "$turnover",
			"pnl":          "$pnl",
		},
		"strategy_info": bson.M{
			"category":   "$strategy_info.category",
			"risk_level": "$strategy_info.risk_level",
		},
		"positions": bson.M{
			"net_quantity": "$positions.net_quantity",
			"avg_price":    "$positions.avg_price",
		},
		"detailed_data": "$detailed_data",
	}}
	return projectionStage
}

func groupbyStage() bson.M {

	groupbyStage := bson.M{"$group": bson.M{
		"_id": bson.M{
			"client_id":     "$client_id",
			"strategy_name": "$strategy_name",
			"symbol":        "$symbol",
		},
		"total_buy":    bson.M{"$sum": bson.M{"$cond": []interface{}{bson.M{"$eq": []interface{}{"$order_type", "BUY"}}, "$quantity", 0}}},
		"total_sell":   bson.M{"$sum": bson.M{"$cond": []interface{}{bson.M{"$eq": []interface{}{"$order_type", "SELL"}}, "$quantity", 0}}},
		"net_quantity": bson.M{"$sum": "$quantity"},
		// "turnover":      bson.M{"$sum": bson.M{"$multiply": []interface{}{"$quantity", "$price"}}},
		// "pnl":           bson.M{"$sum": "$trade_pnl"},
		"strategy_info": bson.M{"$first": "$strategy_info"},
		"positions":     bson.M{"$first": "$positions"},
		"detailed_data": bson.M{"$push": "$trades"},
	}}

	return groupbyStage
}

func unwindStage(filter ReportFilter) []bson.M {

	unwindStage := []bson.M{}

	unwindStage = append(unwindStage, bson.M{"$unwind": "$strategy_info"})

	if filter.IncludePositions {
		unwindStage = append(unwindStage, bson.M{"$unwind": bson.M{"path": "$positions", "preserveNullAndEmptyArrays": true}})
	}
	if filter.IncludeTrades {
		unwindStage = append(unwindStage, bson.M{"$unwind": bson.M{"path": "$trades", "preserveNullAndEmptyArrays": true}})
	}
	return unwindStage
}

func lookupStage() []bson.M {
	lookupStage := []bson.M{
		{
			"$lookup": bson.M{
				"from":         "trades",
				"localField":   "_id",
				"foreignField": "order_id",
				"as":           "trades",
			},
		},
		{
			"$lookup": bson.M{
				"from": "positions",
				"let":  bson.M{"client_id": "$client_id", "symbol": "$symbol"},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": []bson.M{
									{"$eq": []interface{}{"$client_id", "$$client_id"}},
									{"$eq": []interface{}{"$symbol", "$$symbol"}},
								},
							},
						},
					},
				},
				"as": "positions",
			},
		},
		{
			"$lookup": bson.M{
				"from":         "strategy_info",
				"localField":   "strategy_name",
				"foreignField": "strategy_name",
				"as":           "strategy_info",
			},
		},
	}
	return lookupStage
}

func matchStage(filter ReportFilter) bson.M {
	match := bson.M{}
	if len(filter.ClientIDs) > 0 {
		match["client_id"] = bson.M{"$in": filter.ClientIDs}
	}
	if len(filter.StrategyNames) > 0 {
		match["strategy_name"] = bson.M{"$in": filter.StrategyNames}
	}
	if len(filter.Symbols) > 0 {
		match["symbol"] = bson.M{"$in": filter.Symbols}
	}
	if filter.OrderType != "" {
		match["order_type"] = filter.OrderType
	}
	if filter.MinQuantity > 0 || filter.MaxQuantity > 0 {
		q := bson.M{}
		if filter.MinQuantity > 0 {
			q["$gte"] = filter.MinQuantity
		}
		if filter.MaxQuantity > 0 {
			q["$lte"] = filter.MaxQuantity
		}
		match["quantity"] = q
	}
	if filter.MinPrice > 0 || filter.MaxPrice > 0 {
		p := bson.M{}
		if filter.MinPrice > 0 {
			p["$gte"] = filter.MinPrice
		}
		if filter.MaxPrice > 0 {
			p["$lte"] = filter.MaxPrice
		}
		match["price"] = p
	}
	if filter.DateFrom != "" || filter.DateTo != "" {
		dateRange := bson.M{}
		const layout = "2006-01-02"
		if filter.DateFrom != "" {
			if t, err := time.Parse(layout, filter.DateFrom); err == nil {
				dateRange["$gte"] = t
			} else {
				log.Printf("Invalid date_from: %v should be in YYYY-MM-DD format", filter.DateFrom)
			}
		}
		if filter.DateTo != "" {
			if t, err := time.Parse(layout, filter.DateTo); err == nil {
				dateRange["$lte"] = t
			} else {
				log.Printf("Invalid date_to: %v should be in YYYY-MM-DD format", filter.DateTo)
			}
		}
		if len(dateRange) > 0 {
			match["created_at"] = dateRange
		}
	}
	return match
}

// -------------------- Fetch Report --------------------
func FetchReport(filter ReportFilter) []bson.M {
	key := fmt.Sprintf("%v", filter)
	if val, ok := GetFromCache(key); ok {
		fmt.Println("Cache hit for key:", key)
		return val.([]bson.M)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pipeline := BuildPipeline(filter)
	cursor, err := ordersCollection.Aggregate(ctx, pipeline)
	if err != nil {
		log.Println("Mongo Aggregate Error:", err)
		return nil
	}
	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		log.Println("Cursor Error:", err)
		return nil
	}

	// Post-process trades pagination
	for i := range results {
		if trades, ok := results[i]["detailed_data"].(primitive.A); ok && len(trades) > 0 {
			tradeList := []bson.M{}
			for _, t := range trades {
				if trade, ok := t.(bson.M); ok {
					tradeList = append(tradeList, trade)
				}
			}
			// Pagination
			start := (filter.Page - 1) * filter.Limit
			end := start + filter.Limit
			if start > len(tradeList) {
				start = len(tradeList)
			}
			if end > len(tradeList) {
				end = len(tradeList)
			}
			results[i]["detailed_data"] = tradeList[start:end]
		}
	}
	SaveToCache(key, results)

	for i, res := range results {
		fmt.Printf("Result #%d:\n", i+1)
		for k, v := range res {
			fmt.Printf("  %s: %v\n", k, v)
		}
		fmt.Println("-----------------------------")
	}

	return results
}

// -------------------- Worker Pool --------------------
func StartWorkerPool(jobs <-chan Job, results chan<- Result, workers int) {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				data := FetchReport(job.Filter)
				results <- Result{ClientID: job.ClientID, Data: data}
			}
		}()
	}
	wg.Wait()
	close(results)
}

func GenerateReports(filters []ReportFilter) []Result {
	jobs := make(chan Job, len(filters))
	results := make(chan Result, len(filters))
	go StartWorkerPool(jobs, results, 10)
	for _, f := range filters {
		for _, client := range f.ClientIDs {
			jobs <- Job{ClientID: client, Filter: f}
		}
	}
	close(jobs)
	report := []Result{}
	for res := range results {
		report = append(report, res)
	}
	return report
}

// -------------------- Gin Handler --------------------
func ReportHandler(c *gin.Context) {
	var filter ReportFilter
	if err := c.ShouldBindJSON(&filter); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(filter.ClientIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "client_ids is required"})
		return
	}
	report := GenerateReports([]ReportFilter{filter})
	c.JSON(http.StatusOK, gin.H{"data": report})
}

// -------------------- Main --------------------
func main() {
	// Connect MongoDB
	clientOpts := options.Client().ApplyURI("mongodb://root:password@localhost:27017")
	client, err := mongo.Connect(context.TODO(), clientOpts)
	if err != nil {
		log.Fatal(err)
	}
	ordersCollection = client.Database("trading").Collection("orders")
	fmt.Println("MongoDB connected!")

	// Setup Gin
	r := gin.Default()
	r.POST("/daily_report", ReportHandler)
	fmt.Println("Server running on http://localhost:8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
