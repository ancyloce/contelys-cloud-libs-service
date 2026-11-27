package roles

// The generated client is committed under internal/gen/ and never edited by hand. It sits under
// internal/, a level below the layout standard's "gen beside the code that uses it", so that its
// types stay out of the library's API and a change of the contract reaches this package alone.
//go:generate sh -c "go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.0 -config oapi-codegen.yaml \"$(go list -m -f '{{.Dir}}' github.com/ancyloce/contelys-cloud-contracts@v0.1.0)/openapi/organization-access/openapi.yaml\""
