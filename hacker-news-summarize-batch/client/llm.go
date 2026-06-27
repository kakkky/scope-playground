package client

import (
	"context"
	"os"

	"google.golang.org/genai"
)

const model = "gemini-2.5-flash-lite"

type LLMGeminiProvider struct {
	client *genai.Client
}

func NewLLMGeminiProvider() (*LLMGeminiProvider, error) {
	client, err := genai.NewClient(context.Background(), &genai.ClientConfig{
		APIKey: os.Getenv("GEMINI_API_KEY"),
	})
	if err != nil {
		return nil, err
	}
	return &LLMGeminiProvider{
		client: client,
	}, nil
}

func (p *LLMGeminiProvider) Generate(ctx context.Context, prompt string) (string, error) {
	gcc := &genai.GenerateContentConfig{
		ResponseMIMEType: "application/json",
	}
	contents := []*genai.Content{
		genai.NewContentFromText(prompt, genai.RoleUser),
	}
	resp, err := p.client.Models.GenerateContent(ctx, model, contents, gcc)
	if err != nil {
		return "", err
	}
	return resp.Text(), nil
}
