package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ordersCollection *mongo.Collection
var tradesCollection *mongo.Collection
var positionsCollection *mongo.Collection
var strategyInfoCollection *mongo.Collection

func insertInChunks(coll *mongo.Collection, batch []interface{}, chunkSize int) error {
	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}
		_, err := coll.InsertMany(context.Background(), batch[i:end])
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	// ---------------- MongoDB Connection ----------------
	clientOpts := options.Client().ApplyURI("mongodb://root:password@localhost:27017").SetMaxPoolSize(500).SetMinPoolSize(50)
	clientOpts.SetServerSelectionTimeout(20 * time.Second)
	clientOpts.SetSocketTimeout(30 * time.Second)
	clientOpts.SetConnectTimeout(10 * time.Second)
	client, err := mongo.Connect(context.TODO(), clientOpts)
	if err != nil {
		log.Fatal(err)
	}

	ordersCollection = client.Database("trading").Collection("orders")
	tradesCollection = client.Database("trading").Collection("trades")
	positionsCollection = client.Database("trading").Collection("positions")
	strategyInfoCollection = client.Database("trading").Collection("strategy_info")

	fmt.Println("MongoDB connected!")

	// ---------------- Seed Strategy Info ----------------
	strategies := []interface{}{
		map[string]interface{}{"strategy_name": "Straddle", "category": "option_buying", "risk_level": "high"},
		map[string]interface{}{"strategy_name": "Strangle", "category": "option_buying", "risk_level": "medium"},
		map[string]interface{}{"strategy_name": "CoveredCall", "category": "option_selling", "risk_level": "low"},
	}
	insertInChunks(strategyInfoCollection, strategies, 2000)

	// ---------------- Seed Positions ----------------
	clients := []string{}
	for i := 1; i <= 2; i++ { // 50 clients
		clients = append(clients, fmt.Sprintf("C%03d", i))
	}

	var positions []interface{}
	for _, client := range clients {
		for _, s := range []string{"Straddle", "Strangle", "CoveredCall"} {
			pos := map[string]interface{}{
				"client_id":     client,
				"strategy_name": s,
				"symbol":        []string{"APPL", "GOOGL", "TSLA"}[rand.Intn(3)],
				"net_quantity":  rand.Intn(500),
				"avg_price":     float64(80 + rand.Intn(20)),
			}
			positions = append(positions, pos)
		}
	}
	positionsCollection.InsertMany(context.TODO(), positions)

	// ---------------- Seed Orders and Trades ----------------
	totalOrders := 10 // 1 hundred
	batchSize := 5    // insert in batches
	workerCount := 5  // concurrent workers

	jobs := make(chan int, workerCount)
	var wg sync.WaitGroup

	start := time.Now()
	fmt.Printf("Seeding %d orders + %d~ trades using concurrent workers...\n", totalOrders, totalOrders*2)

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batchNum := range jobs {
				orderBatch := []interface{}{}
				tradeBatch := []interface{}{}

				for i := 0; i < batchSize; i++ {
					client := clients[rand.Intn(len(clients))]
					strategy := []string{"Straddle", "Strangle", "CoveredCall"}[rand.Intn(3)]
					symbol := []string{"APPL", "GOOGL", "TSLA"}[rand.Intn(3)]
					orderType := []string{"BUY", "SELL"}[rand.Intn(2)]
					quantity := rand.Intn(100) + 1
					price := float64(80 + rand.Intn(20))
					createdAt := time.Date(2025, 10, rand.Intn(30)+1, rand.Intn(24), rand.Intn(60), rand.Intn(60), 0, time.UTC)

					orderID := primitive.NewObjectID()
					order := map[string]interface{}{
						"_id":           orderID,
						"client_id":     client,
						"strategy_name": strategy,
						"symbol":        symbol,
						"order_type":    orderType,
						"quantity":      quantity,
						"price":         price,
						"created_at":    createdAt,
					}
					orderBatch = append(orderBatch, order)

					// 1-3 trades per order
					tradeCount := rand.Intn(3) + 1
					for t := 0; t < tradeCount; t++ {
						trade := map[string]interface{}{
							"_id":        primitive.NewObjectID(),
							"order_id":   orderID,
							"trade_type": orderType,
							"quantity":   quantity / tradeCount,
							"price":      price + rand.Float64(),
							"timestamp":  createdAt.Add(time.Duration(t) * time.Second),
						}
						tradeBatch = append(tradeBatch, trade)
					}
				}

				// Insert batches concurrently
				if err := insertInChunks(ordersCollection, orderBatch, 2000); err != nil {
					log.Println("Insert orders error:", err)
				}
				if err := insertInChunks(tradesCollection, tradeBatch, 2000); err != nil {
					log.Println("Insert trades error:", err)
				}

				if batchNum%10 == 0 {
					fmt.Printf("Batch %d inserted\n", batchNum)
				}
			}
		}()
	}

	batches := totalOrders / batchSize
	for i := 0; i < batches; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("✅ Seeding completed in %s\n", elapsed)
}
