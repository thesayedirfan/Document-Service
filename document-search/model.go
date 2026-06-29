package main

import "time"

type Document struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenantId"`
	Title     string    `json:"title"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
}

type CreateDocumentRequest struct {
	TenantID string `json:"tenantId" binding:"required"`
	Title    string `json:"title" binding:"required"`
	Content  string `json:"content" binding:"required"`
}

type SearchResult struct {
	ID    string  `json:"id"`
	Title string  `json:"title"`
	Rank  float32 `json:"rank"`
}
