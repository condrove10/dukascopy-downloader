package retryablehttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
)

type Method string

const (
	MethodGet    Method = "GET"
	MethodHead   Method = "HEAD"
	MethodPost   Method = "POST"
	MethodPut    Method = "PUT"
	MethodPatch  Method = "PATCH"
	MethodDelete Method = "DELETE"
)

type Client struct {
	Url            string                                    `validate:"required,http_url"`
	Method         Method                                    `validate:"required"`
	Body           []byte                                    `validate:"required"`
	Header         map[string]string                         `validate:"required"`
	HttpClient     *http.Client                              `validate:"required"`
	context        context.Context                           `validate:"required"`
	maxRetries     uint16                                    `validate:"required,gt=0,lte=65535"`
	retryDelay     time.Duration                             `validate:"required"`
	retryCondition func(resp *http.Response, err error) bool `validate:"required"`
}

var DefaultClient = &Client{
	Body:       make([]byte, 0),
	Header:     make(map[string]string),
	HttpClient: http.DefaultClient,
	context:    context.Background(),
	maxRetries: 0,
	retryDelay: 0,
}

func New() *Client {
	return &Client{}
}

func (c *Client) WithUrl(url string) *Client {
	c.Url = url
	return c
}

func (c *Client) WithMethod(method Method) *Client {
	c.Method = method
	return c
}

func (c *Client) WithBody(body []byte) *Client {
	c.Body = body
	return c
}

func (c *Client) WithHeader(header map[string]string) *Client {
	c.Header = header
	return c
}

func (c *Client) AppendHeader(key, value string) *Client {
	c.Header[key] = value
	return c
}

func (c *Client) WithHttpClient(client *http.Client) *Client {
	c.HttpClient = client
	return c
}

func (c *Client) WithContext(ctx context.Context) *Client {
	c.context = ctx
	return c
}

func (c *Client) WithMaxRetries(maxRetries uint16) *Client {
	c.maxRetries = maxRetries
	return c
}

func (c *Client) WithRetryDelay(delay time.Duration) *Client {
	c.retryDelay = delay
	return c
}

func (c *Client) WithRetryCondition(condition func(resp *http.Response, err error) bool) *Client {
	c.retryCondition = condition
	return c
}

func (c *Client) retry(fn func() (*http.Response, error)) (*http.Response, error) {
	var (
		resp            = &http.Response{}
		err      error  = nil
		retry    bool   = false
		retryErr string = ""
	)

	for i := uint16(0); i < c.maxRetries+1; i += 1 {
		if c.context.Err() != nil {
			err := fmt.Errorf("retryable http call context closed; %v", c.context.Err())
			return nil, err
		}

		if retry {
			time.Sleep(c.retryDelay)
		}

		resp, err = fn()

		retry = c.retryCondition(resp, err)

		if err == nil && !retry {
			return resp, nil
		}

		retryErr += fmt.Sprintf("\n\t\ttry %d: %v; retry condition status: %v", i+1, err, retry)
	}

	return nil, fmt.Errorf("retryable http client max retries exceeded; %s", retryErr)
}

func (c *Client) Do() (*http.Response, error) {
	if err := validator.New().Struct(c); err != nil {
		return nil, fmt.Errorf("validate retryable http client fail; %s", err.Error())
	}

	req, err := http.NewRequestWithContext(c.context, string(c.Method), c.Url, bytes.NewReader(c.Body))
	if err != nil {
		return nil, err
	}

	return c.retry(func() (*http.Response, error) {
		req.Body = io.NopCloser(bytes.NewReader(c.Body))

		for k, v := range c.Header {
			req.Header.Set(k, v)
		}

		return c.HttpClient.Do(req)
	})
}
