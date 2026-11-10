package store

import (
	"context"
	"encoding/json"
)

// AttachChannelTags resolves forum and media posts' applied_tags to the tag
// names in the parent channel's available_tags. Unknown tag IDs are kept as
// IDs so a tag removed from the parent is not silently dropped.
func (s *Store) AttachChannelTags(ctx context.Context, rows []ChannelRow) error {
	if len(rows) == 0 {
		return nil
	}
	index := make(map[string]int, len(rows))
	for i, row := range rows {
		index[row.ID] = i
	}
	query, err := s.db.QueryContext(ctx, `
		select t.id, json_extract(t.raw_json, '$.applied_tags'), coalesce(json_extract(p.raw_json, '$.available_tags'), '[]')
		from channels t
		left join channels p on p.id = coalesce(nullif(t.thread_parent_id, ''), t.parent_id)
		where json_valid(t.raw_json)
		  and json_type(t.raw_json, '$.applied_tags') = 'array'
		  and json_array_length(t.raw_json, '$.applied_tags') > 0`)
	if err != nil {
		return err
	}
	defer func() { _ = query.Close() }()
	for query.Next() {
		var id, appliedJSON, availableJSON string
		if err := query.Scan(&id, &appliedJSON, &availableJSON); err != nil {
			return err
		}
		i, ok := index[id]
		if !ok {
			continue
		}
		var applied []string
		var available []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		if json.Unmarshal([]byte(appliedJSON), &applied) != nil {
			continue
		}
		_ = json.Unmarshal([]byte(availableJSON), &available)
		names := make(map[string]string, len(available))
		for _, tag := range available {
			names[tag.ID] = tag.Name
		}
		tags := make([]string, 0, len(applied))
		for _, tagID := range applied {
			if name := names[tagID]; name != "" {
				tags = append(tags, name)
			} else if tagID != "" {
				tags = append(tags, tagID)
			}
		}
		if len(tags) > 0 {
			rows[i].Tags = tags
		}
	}
	return query.Err()
}
