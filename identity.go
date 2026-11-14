package uvim

import (
	"fmt"
	"strings"
	"time"
)

// IdentityEventRef identifies a connector-owned message. A caller cannot choose
// the native user or send address instead of proving this event's origin.
type IdentityEventRef struct {
	Provider  string `json:"provider"`
	Connector string `json:"connector"`
	EventID   string `json:"event_id"`
}

type IdentityLinkRef struct {
	ID       string `json:"id"`
	Revision string `json:"revision"`
}

type IdentityLink struct {
	IdentityLinkRef
	Identity   UserIdentity     `json:"identity"`
	Account    *UserIdentity    `json:"account,omitempty"`
	Provider   string           `json:"provider"`
	Connector  string           `json:"connector"`
	UserID     string           `json:"user_id"`
	Target     OutboundTarget   `json:"target"`
	Origin     IdentityEventRef `json:"origin"`
	Proof      string           `json:"proof"`
	VerifiedAt time.Time        `json:"verified_at"`
	Revoked    bool             `json:"revoked,omitempty"`
}

type IdentityLinkRequest struct {
	Identity UserIdentity     `json:"identity"`
	Account  *UserIdentity    `json:"account,omitempty"`
	Event    IdentityEventRef `json:"event"`
	Proof    string           `json:"proof,omitempty"`
}

type IdentityResolveRequest struct {
	Identity  UserIdentity      `json:"identity"`
	Provider  string            `json:"provider"`
	Connector string            `json:"connector"`
	Event     *IdentityEventRef `json:"event,omitempty"`
}

type IdentityResolveResult struct {
	Found   bool          `json:"found"`
	Revoked bool          `json:"revoked,omitempty"`
	Link    *IdentityLink `json:"link,omitempty"`
}

// IdentityInboundResolveRequest names only connector-owned durable evidence.
// Native actor, business identities and private targets cannot be supplied.
type IdentityInboundResolveRequest struct {
	Provider  string `json:"provider"`
	Connector string `json:"connector"`
	EventID   string `json:"event_id"`
}

type IdentityNativeActor struct {
	Provider  string `json:"provider"`
	Connector string `json:"connector"`
	UserID    string `json:"user_id"`
	Kind      string `json:"kind"`
}

type IdentityInboundLink struct {
	Identity UserIdentity    `json:"identity"`
	Link     IdentityLinkRef `json:"link"`
	Target   OutboundTarget  `json:"target"`
}

// IdentityInboundResolveResult is service-private. Links is the complete
// currently valid canonical identity set for this native actor, never an
// implicit preferred identity. DirectTarget is absent for a group event.
type IdentityInboundResolveResult struct {
	Event        IdentityEventRef      `json:"event"`
	EventDigest  string                `json:"event_digest"`
	NativeActor  IdentityNativeActor   `json:"native_actor"`
	DirectTarget *OutboundTarget       `json:"direct_target"`
	Links        []IdentityInboundLink `json:"links"`
}

// NormalizeUserIdentities omits absent identifiers and preserves the first
// assertion for each type/scope pair. Native identifiers are not case folded.
func NormalizeUserIdentities(values ...UserIdentity) []UserIdentity {
	var identities []UserIdentity
	seen := make(map[[2]string]bool, len(values))
	for _, identity := range values {
		identity.Type = strings.TrimSpace(identity.Type)
		if identity.Type == "" || strings.TrimSpace(identity.ID) == "" {
			continue
		}
		key := [2]string{identity.Type, identity.Scope}
		if seen[key] {
			continue
		}
		seen[key] = true
		if identity.Source == "" {
			identity.Source = IdentitySourceCallback
		}
		identities = append(identities, identity)
	}
	return identities
}

// UserIdentityMapping asserts an operator-verified association for one exact
// account. Names, conversation IDs and wildcard selectors are not lookup keys.
type UserIdentityMapping struct {
	Provider   string         `json:"provider"`
	Connector  string         `json:"connector"`
	UserID     string         `json:"user_id"`
	Identities []UserIdentity `json:"identities"`
}

// UserIdentityMapper is immutable after construction and safe for concurrent use.
type UserIdentityMapper struct {
	entries map[[3]string][]UserIdentity
}

func NewUserIdentityMapper(mappings []UserIdentityMapping) (*UserIdentityMapper, error) {
	mapper := &UserIdentityMapper{entries: make(map[[3]string][]UserIdentity, len(mappings))}
	for index, mapping := range mappings {
		if strings.TrimSpace(mapping.Provider) == "" || strings.TrimSpace(mapping.Connector) == "" || strings.TrimSpace(mapping.UserID) == "" || len(mapping.Identities) == 0 {
			return nil, fmt.Errorf("identity mapping %d requires provider, connector, user_id and identities", index)
		}
		key := [3]string{mapping.Provider, mapping.Connector, mapping.UserID}
		if _, exists := mapper.entries[key]; exists {
			return nil, fmt.Errorf("identity mapping %d repeats an account selector", index)
		}
		identities := append([]UserIdentity(nil), mapping.Identities...)
		seen := make(map[[2]string]bool, len(identities))
		for i := range identities {
			identity := &identities[i]
			identity.Type = strings.TrimSpace(identity.Type)
			identityKey := [2]string{identity.Type, identity.Scope}
			if identity.Type == "" || strings.TrimSpace(identity.ID) == "" || seen[identityKey] {
				return nil, fmt.Errorf("identity mapping %d requires nonempty, distinct type/scope identifiers", index)
			}
			seen[identityKey] = true
			identity.Source = IdentitySourceConfigured
		}
		mapper.entries[key] = identities
	}
	return mapper, nil
}

// Apply appends configured user identities without changing native routing or
// replacing platform assertions. Non-account subjects cannot receive bindings.
func (m *UserIdentityMapper) Apply(event Event) Event {
	if m == nil || event.User.ID == "" {
		return event
	}
	switch event.User.Kind {
	case "", UserKindUser, UserKindBot:
	default:
		return event
	}
	identities := m.entries[[3]string{event.Provider, event.Connector, event.User.ID}]
	if len(identities) != 0 {
		values := append([]UserIdentity(nil), event.User.Identities...)
		event.User.Identities = NormalizeUserIdentities(append(values, identities...)...)
	}
	return event
}
