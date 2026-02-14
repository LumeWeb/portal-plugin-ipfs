A website has been updated on the IPFS hosting platform.

Domain: {{.Domain}}
User Email: {{.UserEmail}}
Updated At: {{.UpdatedAt}}

Changes:
{{range $key, $value := .Changes}}
  - {{$key}}: {{$value}}
{{end}}

Target Type: {{.TargetType}}
Target Hash: {{.TargetHash}}
Status: {{.Status}}

--
IPFS Website Hosting
