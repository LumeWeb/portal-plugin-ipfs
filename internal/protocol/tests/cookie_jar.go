// Package tests implements a more permissive cookie jar for testing
// that allows localhost to set cookies on subdomains like account.localhost
package tests

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// CookieJar implements http.CookieJar with relaxed domain validation
type CookieJar struct {
	mu      sync.Mutex
	cookies map[string][]*http.Cookie
}

// NewCookieJar creates a new cookie jar
func NewCookieJar() *CookieJar {
	return &CookieJar{
		cookies: make(map[string][]*http.Cookie),
	}
}

// SetCookies implements the http.CookieJar interface
func (j *CookieJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if u.Scheme != "http" && u.Scheme != "https" {
		return
	}

	host := u.Host
	if strings.Contains(host, ":") {
		host = strings.Split(host, ":")[0]
	}

	// Store cookies for this host
	j.cookies[host] = append(j.cookies[host], cookies...)
}

// Cookies implements the http.CookieJar interface
func (j *CookieJar) Cookies(u *url.URL) []*http.Cookie {
	j.mu.Lock()
	defer j.mu.Unlock()

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil
	}

	host := u.Host
	if strings.Contains(host, ":") {
		host = strings.Split(host, ":")[0]
	}

	var result []*http.Cookie
	
	// Add cookies for exact host match
	if cookies, ok := j.cookies[host]; ok {
		result = append(result, filterValidCookies(cookies)...)
	}

	// For subdomain hosts, also check if there are cookies from parent domains
	// This allows account.localhost to receive cookies set by localhost
	if strings.Contains(host, ".") {
		parts := strings.Split(host, ".")
		for i := 1; i < len(parts); i++ {
			domain := strings.Join(parts[i:], ".")
			if cookies, ok := j.cookies[domain]; ok {
				// Filter cookies that should be sent to this host
				for _, cookie := range cookies {
					if j.shouldSendCookie(cookie, u.Scheme, host, u.Path) {
						result = append(result, cookie)
					}
				}
			}
		}
	}

	return result
}

// shouldSendCookie determines if a cookie should be sent to the given host and path
func (j *CookieJar) shouldSendCookie(cookie *http.Cookie, scheme, host, path string) bool {
	// Check path matching
	if cookie.Path != "" && !strings.HasPrefix(path, cookie.Path) {
		return false
	}

	// Check secure flag
	if cookie.Secure && scheme != "https" {
		return false
	}

	// Check if cookie is expired
	if !cookie.Expires.IsZero() && cookie.Expires.Before(time.Now()) {
		return false
	}

	return true
}

// filterValidCookies removes expired cookies
func filterValidCookies(cookies []*http.Cookie) []*http.Cookie {
	var valid []*http.Cookie
	now := time.Now()
	
	for _, cookie := range cookies {
		if cookie.Expires.IsZero() || cookie.Expires.After(now) {
			valid = append(valid, cookie)
		}
	}
	
	return valid
}
