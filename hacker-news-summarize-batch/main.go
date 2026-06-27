package main

import (
	"batch/client"
	"context"
	"flag"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/arunsworld/nursery"
	"github.com/kakkky/scope"
	"github.com/sourcegraph/conc/pool"
	"golang.org/x/sync/errgroup"
)

type result struct {
	id      int
	title   string
	summary string
}

func main() {
	llm, err := client.NewLLMGeminiProvider()
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	hackerNewsClient := client.NewHackerNews()

	pkg := flag.String("pkg", "kakkky/scope", "構造的並行処理に利用するpackage")
	flag.Parse()

	ctx := context.Background()

	switch *pkg {
	case "kakkky/scope":
		var results []result
		err := scope.Run(ctx, func(s *scope.Scope) error {
			idsF := scope.GoFuture(s, func(ctx context.Context) ([]int, error) {
				return fetchTopStoryIDs(hackerNewsClient, 10)
			})
			ids, err := idsF.Wait()
			if err != nil {
				return err
			}
			results = make([]result, len(ids))
			s.Scope(func(child *scope.Scope) error {
				for i, id := range ids {
					i, id := i, id
					child.Go(func(ctx context.Context) error {
						title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
						if err != nil {
							return err
						}
						results[i] = result{id: id, title: title, summary: summary}
						return nil
					})
				}
				return nil
			}, scope.WithMaxConcurrency(5))
			return nil
		})
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		printResults(results)

	case "errgroup":
		ids, err := fetchTopStoryIDs(hackerNewsClient, 10)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		results := make([]result, len(ids))
		sem := make(chan struct{}, 5)
		g, ctx := errgroup.WithContext(ctx)
		for i, id := range ids {
			g.Go(func() error {
				sem <- struct{}{}
				defer func() { <-sem }()
				title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
				if err != nil {
					return err
				}
				results[i] = result{id: id, title: title, summary: summary}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			fmt.Println("error:", err)
			return
		}
		printResults(results)

	case "conc":
		ids, err := fetchTopStoryIDs(hackerNewsClient, 10)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		results := make([]result, len(ids))
		p := pool.New().WithMaxGoroutines(5).WithErrors().WithContext(ctx)
		for i, id := range ids {
			p.Go(func(ctx context.Context) error {
				title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
				if err != nil {
					return err
				}
				results[i] = result{id: id, title: title, summary: summary}
				return nil
			})
		}
		if err := p.Wait(); err != nil {
			fmt.Println("error:", err)
			return
		}
		printResults(results)

	case "nursery":
		ids, err := fetchTopStoryIDs(hackerNewsClient, 10)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		results := make([]result, len(ids))
		jobs := make([]nursery.ConcurrentJob, len(ids))
		for i, id := range ids {
			jobs[i] = func(ctx context.Context, errCh chan error) {
				title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
				if err != nil {
					errCh <- err
					return
				}
				results[i] = result{id: id, title: title, summary: summary}
			}
		}
		if err := nursery.RunConcurrentlyWithContext(ctx, jobs...); err != nil {
			fmt.Println("error:", err)
			return
		}
		printResults(results)

	default: // raw goroutine
		ids, err := fetchTopStoryIDs(hackerNewsClient, 10)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		results := make([]result, len(ids))
		sem := make(chan struct{}, 5)
		var wg sync.WaitGroup
		var firstErr error
		var mu sync.Mutex
		for i, id := range ids {
			i, id := i, id
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
						}
						mu.Unlock()
					}
				}()
				sem <- struct{}{}
				defer func() { <-sem }()
				title, summary, err := fetchAndSummarize(ctx, hackerNewsClient, llm, id)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				results[i] = result{id: id, title: title, summary: summary}
			}()
		}
		wg.Wait()
		if firstErr != nil {
			fmt.Println("error:", firstErr)
			return
		}
		printResults(results)
	}
}

func printResults(results []result) {
	for i, res := range results {
		fmt.Printf("#%d タイトル: %s\n   ID: %d\n   サマリ: %s\n\n", i+1, res.title, res.id, res.summary)
	}
}

func fetchTopStoryIDs(c *client.HackerNews, limit int) ([]int, error) {
	res, err := c.ListTopStories()
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
	item, err := c.GetItem(id)
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
