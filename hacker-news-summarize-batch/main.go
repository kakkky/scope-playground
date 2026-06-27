package main

import (
	"batch/client"
	"context"
	"flag"
	"fmt"

	"github.com/kakkky/scope"
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
	}

	hackerNewsClient := client.NewHackerNews()

	pkg := flag.String("pkg", "kakkky/scope", "構造的並行処理に利用するpackage")
	flag.Parse()

	ctx := context.Background()

	switch *pkg {
	case "errgroup":
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
		for i, res := range results {
			fmt.Printf("[%d] %s (id: %d)\n%s\n\n", i+1, res.title, res.id, res.summary)
		}
	case "conc":
	case "nurnsely":
	default:

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
