package build

import (
	"go.lumeweb.com/portal/build"
)

var (
	Version      string
	GitCommit    string
	GitBranch    string
	BuildTime    string
	GoVersion    string
	Platform     string
	Architecture string
)

func GetInfo() build.BuildInfo {
	return build.New(Version, GitCommit, GitBranch, BuildTime, GoVersion, Platform, Architecture)
}
