package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const hackerNewsBaseURL = "https://hacker-news.firebaseio.com/v0"

type HackerNews struct {
	client *http.Client
}

func NewHackerNews() *HackerNews {
	return &HackerNews{
		client: http.DefaultClient,
	}
}

type ListTopStoriesResponse []int

func (h *HackerNews) ListTopStories() (ListTopStoriesResponse, error) {
	url := fmt.Sprintf("%s/%s", hackerNewsBaseURL, "beststories.json")
	resp, err := h.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var res ListTopStoriesResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}
	return res, nil
}

type GetItemResponse struct {
	By    string `json:"by"`
	Time  int64  `json:"time"`
	Title string `json:"title"`
	URL   string `json:"url"`
}

func (h *HackerNews) GetItem(id int) (GetItemResponse, error) {
	url := fmt.Sprintf("%s/item/%d.json", hackerNewsBaseURL, id)
	resp, err := h.client.Get(url)
	if err != nil {
		return GetItemResponse{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return GetItemResponse{}, err
	}
	var res GetItemResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return GetItemResponse{}, err
	}
	return res, nil
}
