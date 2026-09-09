package share

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Filtered snapshots carry membership and channel scope in their own tables.
// Guild raw payloads must not provide a second, unfiltered copy of that scope.
func projectPublishedGuild(row map[string]any) error {
	var payload struct {
		Roles []struct {
			ID          string `json:"id"`
			Permissions any    `json:"permissions"`
		} `json:"roles"`
	}
	if err := decodeJSONUseNumber(stringValue(row["raw_json"]), &payload); err != nil {
		return fmt.Errorf("decode published guild permissions: %w", err)
	}
	roles := make([]map[string]string, 0, 1)
	for _, role := range payload.Roles {
		if role.ID != stringValue(row["id"]) {
			continue
		}
		permissions, ok := parsePermissionBits(role.Permissions)
		if !ok {
			return errors.New("invalid published guild permissions")
		}
		roles = append(roles, map[string]string{
			"id": role.ID, "permissions": strconv.FormatInt(permissions, 10),
		})
	}
	raw, err := json.Marshal(map[string]any{
		"id": stringValue(row["id"]), "name": stringValue(row["name"]),
		"icon": stringValue(row["icon"]), "roles": roles,
	})
	if err != nil {
		return err
	}
	row["raw_json"] = string(raw)
	return nil
}
