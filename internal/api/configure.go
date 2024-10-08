package api

import "go.lumeweb.com/portal/config"

var _ config.Reconfigurable = (*API)(nil)

func (a *API) Reconfigure(_ config.Scope, _ any) error {
	return nil
}
