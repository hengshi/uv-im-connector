package uvim

import (
	"fmt"
	"strings"
)

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
