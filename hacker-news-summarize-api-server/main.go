package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"server/client"
	"strconv"
	"sync"

	"github.com/arunsworld/nursery"
	"github.com/kakkky/scope"
	"github.com/sourcegraph/conc/pool"
	"golang.org/x/sync/errgroup"
)

type result struct {
	ID      int    `json:"id"`
	Title   string `json:"title"`
	Summary string `json:"summary"`
}

func main() {
	llm, err := client.NewLLMGeminiProvider()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	hackerNewsClient := client.NewHackerNews()

	http.HandleFunc("/hacker_news/trend_summary", trendSummaryHandler(hackerNewsClient, llm))

	fmt.Println("listening on :8080")
	http.ListenAndServe(":8080", nil)
}

func trendSummaryHandler(hackerNewsClient *client.HackerNews, llm *client.LLMGeminiProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := 10
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil {
				limit = n
			}
		}
		pkg := r.URL.Query().Get("pkg")

		ctx := r.Context()
		ids, err := fetchTopStoryIDs(ctx, hackerNewsClient, limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var results []result
		var handlerErr error

		switch pkg {
		case "scope":
			resultCh := make(chan result, len(ids))
			err := scope.Run(ctx, func(s *scope.Scope) error {
				for _, id := range ids {
					s.Go(func(ctx context.Context) error {
						title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
						if err != nil {
							return err
						}
						resultCh <- result{ID: id, Title: title, Summary: summary}
						fmt.Printf("[done] id=%d\n", id)
						return nil
					})
				}
				return nil
			})
			close(resultCh)
			for r := range resultCh {
				results = append(results, r)
			}
			handlerErr = err
		case "errgroup":
			resultCh := make(chan result, len(ids))
			g, ctx := errgroup.WithContext(ctx)
			for _, id := range ids {
				g.Go(func() error {
					title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
					if err != nil {
						return err
					}
					resultCh <- result{ID: id, Title: title, Summary: summary}
					fmt.Printf("[done] id=%d\n", id)
					return nil
				})
			}
			handlerErr = g.Wait()
			close(resultCh)
			for r := range resultCh {
				results = append(results, r)
			}

		case "conc":
			resultCh := make(chan result, len(ids))
			p := pool.New().WithErrors().WithContext(ctx)
			for _, id := range ids {
				p.Go(func(ctx context.Context) error {
					title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
					if err != nil {
						return err
					}
					resultCh <- result{ID: id, Title: title, Summary: summary}
					fmt.Printf("[done] id=%d\n", id)
					return nil
				})
			}
			handlerErr = p.Wait()
			close(resultCh)
			for r := range resultCh {
				results = append(results, r)
			}

		case "nursery":
			resultCh := make(chan result, len(ids))
			jobs := make([]nursery.ConcurrentJob, len(ids))
			for i, id := range ids {
				jobs[i] = func(ctx context.Context, errCh chan error) {
					title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
					if err != nil {
						errCh <- err
						return
					}
					resultCh <- result{ID: id, Title: title, Summary: summary}
					fmt.Printf("[done] id=%d\n", id)
				}
			}
			handlerErr = nursery.RunConcurrentlyWithContext(ctx, jobs...)
			close(resultCh)
			for r := range resultCh {
				results = append(results, r)
			}

		default: // raw goroutine
			resultCh := make(chan result, len(ids))
			var wg sync.WaitGroup
			var firstErr error
			var mu sync.Mutex
			for _, id := range ids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
					if err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
						return
					}
					resultCh <- result{ID: id, Title: title, Summary: summary}
					fmt.Printf("[done] id=%d\n", id)
				}()
			}
			wg.Wait()
			close(resultCh)
			for r := range resultCh {
				results = append(results, r)
			}
			handlerErr = firstErr
		}

		if handlerErr != nil {
			http.Error(w, handlerErr.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}
}

func fetchTopStoryIDs(ctx context.Context, c *client.HackerNews, limit int) ([]int, error) {
	res, err := c.ListTopStories(ctx)
	if err != nil {
		return nil, err
	}
	ids := []int(res)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func fetchAndSummarize(ctx context.Context, c *client.HackerNews, llm *client.LLMGeminiProvider, id int) (title, summary string, err error) {
	item, err := c.GetItem(ctx, id)
	if err != nil {
		return "", "", err
	}
	prompt := fmt.Sprintf(
		"以下のHacker News記事を日本語で3文以内で要約してください。\nタイトル: %s\nURL: %s",
		item.Title, item.URL,
	)
	summary, err = llm.Generate(ctx, prompt)
	if err != nil {
		return "", "", err
	}
	return item.Title, summary, nil
}
