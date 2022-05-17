package tenant

type (
	// TenantID is the tenant id type
	TenantID = string
	// ProjectID is the project id type of tenant
	ProjectID = string
)

// tenant const variables
const (
	FrameTenantID       = "dfe_root"
	TestTenantID        = "dfe_test"
	DefaultUserTenantID = "def_default_user"
)

// ProjectInfo is the tenant/project information which is consistent with DBaas
type ProjectInfo struct {
	TenantID  TenantID
	ProjectID ProjectID
}
