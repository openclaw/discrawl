package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/bwmarrin/discordgo"
)

// Change only the role identified by an actual Gateway role event. Other guild
// fields and retained channel metadata are not overwritten by a partial event.
func (s *Store) ApplyGuildRole(ctx context.Context, guildID, roleID string, role *discordgo.Role) error {
	if guildID == "" || roleID == "" || (role != nil && role.ID != roleID) {
		return errors.New("role identity required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var raw string
	if err = tx.QueryRowContext(ctx, `select raw_json from guilds where id=?`, guildID).Scan(&raw); err != nil {
		return err
	}
	var guild map[string]json.RawMessage
	if err = json.Unmarshal([]byte(raw), &guild); err != nil {
		return err
	}
	var roles []json.RawMessage
	if b := guild["roles"]; len(b) > 0 {
		if err = json.Unmarshal(b, &roles); err != nil {
			return err
		}
	}
	out := make([]json.RawMessage, 0, len(roles)+1)
	for _, r := range roles {
		var id struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(r, &id); err != nil {
			return err
		}
		if id.ID != roleID {
			out = append(out, r)
		}
	}
	if role != nil {
		b, err := json.Marshal(role)
		if err != nil {
			return err
		}
		out = append(out, b)
	}
	guild["roles"], err = json.Marshal(out)
	if err != nil {
		return err
	}
	body, err := json.Marshal(guild)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `update guilds set raw_json=?,updated_at=? where id=?`, string(body), time.Now().UTC().Format(timeLayout), guildID); err != nil {
		return err
	}
	return tx.Commit()
}
