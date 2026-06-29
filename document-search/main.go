package main

import (
	"log"

	"github.com/gin-gonic/gin"
)

func main() {

	db, err := NewDB()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	repo := NewRepository(db)
	handler := NewHandler(repo)

	router := gin.Default()

	router.POST("/documents", handler.CreateDocument)
	router.GET("/documents/:id", handler.GetDocument)
	router.DELETE("/documents/:id", handler.DeleteDocument)
	router.GET("/search", handler.Search)
	router.GET("/health", handler.Health)

	log.Println("Server running on :8080")

	router.Run(":8080")
}
