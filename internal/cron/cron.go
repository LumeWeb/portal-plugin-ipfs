package cron

import (
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/tasks"
	"go.lumeweb.com/portal/core"
)

var _ core.Cronable = (*Cron)(nil)

type Cron struct {
}

func (c Cron) RegisterTasks(crn core.CronService) error {
	crn.RegisterTask(define.CronTaskPinName, core.CronTaskFuncHandler[*define.CronTaskPinArgs](tasks.CronTaskPin), core.CronTaskDefinitionOneTimeJob, define.CronTaskPinArgsFactory, false)
	crn.RegisterTask(define.CronTaskTusUploadName, core.CronTaskFuncHandler[*define.CronTaskTusUploadArgs](tasks.CronTaskTusUpload), core.CronTaskDefinitionOneTimeJob, define.CronTaskTusUploadArgsFactory, false)
	crn.RegisterTask(define.CronTaskPostUploadName, core.CronTaskFuncHandler[*define.CronTaskPostUploadArgs](tasks.CronTaskPostUpload), core.CronTaskDefinitionOneTimeJob, define.CronTaskPostUploadArgsFactory, false)
	crn.RegisterTask(define.CronTaskPostUploadCleanupName, core.CronTaskFuncHandler[*define.CronTaskPostUploadCleanupArgs](tasks.CronTaskPostUploadCleanup), core.CronTaskDefinitionOneTimeJob, define.CronTaskPostUploadCleanupArgsFactory, false)
	crn.RegisterTask(define.CronTaskTusUploadCleanupName, core.CronTaskFuncHandler[*define.CronTaskTusUploadCleanupArgs](tasks.CronTaskTusUploadCleanup), core.CronTaskDefinitionOneTimeJob, define.CronTaskTusUploadCleanupArgsFactory, false)
	return nil
}

func (c Cron) ScheduleJobs(_ core.CronService) error {
	return nil
}

func NewCron(_ core.Context) *Cron {
	return &Cron{}
}
