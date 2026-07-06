package uvim

import (
	"runtime/debug"
	"strings"
)

const (
	ServiceName     = "uv-im-connector"
	ProtocolVersion = "v1"
)

var (
	Version   = "dev"
	GitCommit = ""
	BuildTime = ""
)

type BuildMetadata struct {
	Version     string `json:"connector_version"`
	GitCommit   string `json:"git_commit,omitempty"`
	BuildTime   string `json:"build_time,omitempty"`
	VCSModified bool   `json:"vcs_modified,omitempty"`
}

func RuntimeBuildMetadata() BuildMetadata {
	meta := BuildMetadata{
		Version:   strings.TrimSpace(Version),
		GitCommit: strings.TrimSpace(GitCommit),
		BuildTime: strings.TrimSpace(BuildTime),
	}
	if meta.Version == "" {
		meta.Version = "dev"
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return meta
	}
	if (meta.Version == "dev" || meta.Version == "(devel)") && info.Main.Version != "" && info.Main.Version != "(devel)" {
		meta.Version = info.Main.Version
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if meta.GitCommit == "" {
				meta.GitCommit = setting.Value
			}
		case "vcs.time":
			if meta.BuildTime == "" {
				meta.BuildTime = setting.Value
			}
		case "vcs.modified":
			meta.VCSModified = setting.Value == "true"
		}
	}
	return meta
}

type ServiceMeta struct {
	Service          string         `json:"service"`
	ConnectorVersion string         `json:"connector_version"`
	ProtocolVersion  string         `json:"protocol_version"`
	GitCommit        string         `json:"git_commit,omitempty"`
	BuildTime        string         `json:"build_time,omitempty"`
	VCSModified      bool           `json:"vcs_modified,omitempty"`
	Providers        []ProviderMeta `json:"providers"`
}

type ProviderMeta struct {
	Provider     string       `json:"provider"`
	Connector    string       `json:"connector,omitempty"`
	Capabilities Capabilities `json:"capabilities"`
	Health       Health       `json:"health"`
}

func NewServiceMeta(providers []ProviderMeta) ServiceMeta {
	build := RuntimeBuildMetadata()
	return ServiceMeta{
		Service:          ServiceName,
		ConnectorVersion: build.Version,
		ProtocolVersion:  ProtocolVersion,
		GitCommit:        build.GitCommit,
		BuildTime:        build.BuildTime,
		VCSModified:      build.VCSModified,
		Providers:        providers,
	}
}
