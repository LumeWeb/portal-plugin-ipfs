The status of your website has changed.

Domain: {{.Domain}}
Previous Status: {{.OldStatus}}
New Status: {{.NewStatus}}
Changed At: {{.ChangedAt}}
Target Type: {{.TargetType}}
Target Hash: {{.TargetHash}}

{{if eq .NewStatus "broken"}}
Your website has been marked as broken because its target is no longer valid or available.
Please check your IPFS pins or IPNS keys and update the website configuration if needed.
{{else if eq .NewStatus "active"}}
Your website is now active and being served.
{{end}}

--
IPFS Website Hosting
