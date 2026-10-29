package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

const directoryCacheMax = 1024

type directoryEntry struct {
	userID    string
	name      string
	expiresAt time.Time
}

type directoryResponse struct {
	ErrCode     *int   `json:"errcode"`
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	UserID      string `json:"userid"`
	Name        string `json:"name"`
	Users       []struct {
		OpenUserID string `json:"open_userid"`
		UserID     string `json:"userid"`
	} `json:"userid_list"`
	Invalid []string `json:"invalid_open_userid_list"`
}

// Enrichment runs in the serial emitter, never in the socket ACK dispatcher.
func (p *Provider) enrichDirectoryUser(ctx context.Context, event *uvim.Event) {
	if p.config.CorpID == "" || event.User.ID == "" || (event.User.Kind != "" && event.User.Kind != uvim.UserKindUser) {
		return
	}
	p.directoryMu.Lock()
	defer p.directoryMu.Unlock()
	entry, cached := p.directoryUsers[event.User.ID]
	if !cached || !p.now().Before(entry.expiresAt) {
		lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var err error
		entry, err = p.directoryUser(lookupCtx, event.User.ID)
		cancel()
		if ctx.Err() != nil {
			return
		}
		ttl := 10 * time.Minute
		if err != nil {
			ttl = 30 * time.Second
			p.config.Logger.Warn("wecom directory lookup failed", "err", err.Error())
		}
		entry.expiresAt = p.now().Add(ttl)
		if len(p.directoryUsers) >= directoryCacheMax {
			for key, old := range p.directoryUsers {
				if !p.now().Before(old.expiresAt) {
					delete(p.directoryUsers, key)
				}
			}
			if len(p.directoryUsers) >= directoryCacheMax {
				for key := range p.directoryUsers {
					delete(p.directoryUsers, key)
					break
				}
			}
		}
		p.directoryUsers[event.User.ID] = entry
	}
	if entry.userID != "" {
		event.User.Identities = uvim.NormalizeUserIdentities(append(event.User.Identities, uvim.UserIdentity{
			Type: "wecom.userid", ID: entry.userID, Scope: p.config.CorpID, Source: uvim.IdentitySourceLookup,
		})...)
		event.User.DisplayName = uvim.FirstNonEmpty(event.User.DisplayName, event.User.Name, entry.name)
	}
}

func (p *Provider) directoryUser(ctx context.Context, nativeID string) (directoryEntry, error) {
	converted, err := p.directoryCall(ctx, "batch/openuserid_to_userid", nil, map[string]any{"open_userid_list": []string{nativeID}})
	if err != nil {
		return directoryEntry{}, err
	}
	entry := directoryEntry{}
	for _, user := range converted.Users {
		if user.OpenUserID == nativeID && user.UserID != "" {
			entry.userID = user.UserID
			break
		}
	}
	if entry.userID == "" {
		for _, invalid := range converted.Invalid {
			if invalid == nativeID {
				// Invalid conversion does not prove plaintext. An exact member
				// lookup must succeed before asserting this enterprise identity.
				profile, err := p.directoryCall(ctx, "user/get", url.Values{"userid": {nativeID}}, nil)
				if err != nil {
					return entry, err
				}
				if profile.UserID == "" || !strings.EqualFold(profile.UserID, nativeID) {
					return entry, fmt.Errorf("wecom directory user/get: member identity mismatch")
				}
				return directoryEntry{userID: profile.UserID, name: profile.Name}, nil
			}
		}
		return entry, fmt.Errorf("wecom directory conversion: no matching identity")
	}
	profile, err := p.directoryCall(ctx, "user/get", url.Values{"userid": {entry.userID}}, nil)
	if err != nil {
		// The authenticated conversion is enough to prove the account even
		// when the application cannot read its display metadata.
		return entry, err
	}
	if !strings.EqualFold(profile.UserID, entry.userID) {
		return entry, fmt.Errorf("wecom directory user/get: member identity mismatch")
	}
	entry.name = profile.Name
	return entry, nil
}

func (p *Provider) directoryCall(ctx context.Context, path string, query url.Values, body any) (directoryResponse, error) {
	if query == nil {
		query = url.Values{}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if p.directoryToken == "" || !p.now().Before(p.directoryExpires) {
			token, err := p.directoryJSON(ctx, "gettoken", url.Values{"corpid": {p.config.CorpID}, "corpsecret": {p.config.CorpSecret}}, nil)
			if err != nil {
				return directoryResponse{}, err
			}
			if token.AccessToken == "" || token.ExpiresIn <= 0 {
				return directoryResponse{}, fmt.Errorf("wecom directory gettoken: missing token or lifetime")
			}
			lifetime := time.Duration(token.ExpiresIn) * time.Second
			p.directoryToken = token.AccessToken
			p.directoryExpires = p.now().Add(lifetime - min(time.Minute, lifetime/10))
		}
		query.Set("access_token", p.directoryToken)
		result, err := p.directoryJSON(ctx, path, query, body)
		if result.ErrCode != nil && (*result.ErrCode == 40014 || *result.ErrCode == 42001) {
			p.directoryToken = ""
			continue
		}
		return result, err
	}
	return directoryResponse{}, fmt.Errorf("wecom directory: application token rejected after refresh")
}

// Never return HTTP client errors, raw response bodies or platform errmsg:
// request URLs contain credentials, and platform diagnostics can echo them.
func (p *Provider) directoryJSON(ctx context.Context, path string, query url.Values, body any) (directoryResponse, error) {
	var result directoryResponse
	var payload io.Reader
	method := http.MethodGet
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return result, fmt.Errorf("wecom directory %s: invalid request body", path)
		}
		payload, method = bytes.NewReader(encoded), http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, "https://qyapi.weixin.qq.com/cgi-bin/"+path+"?"+query.Encode(), payload)
	if err != nil {
		return result, fmt.Errorf("wecom directory %s: invalid request", path)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return result, fmt.Errorf("wecom directory %s: request failed or timed out", path)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("wecom directory %s: HTTP %d", path, resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil || result.ErrCode == nil {
		return result, fmt.Errorf("wecom directory %s: invalid response", path)
	}
	if *result.ErrCode != 0 {
		return result, fmt.Errorf("wecom directory %s: errcode=%d", path, *result.ErrCode)
	}
	return result, nil
}
