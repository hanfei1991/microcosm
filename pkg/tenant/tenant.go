package tenant

type TenantID = string
type ProjectID = string

// tenant const variables
const (
	FrameTenantID       = "dfe_root"
	TestTenantID        = "dfe_test"
	DefaultUserTenantID = "def_default_user"
)

type ProjectInfo struct {
	tenantID  TenantID
	projectID ProjectID
}
