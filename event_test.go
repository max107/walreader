package walreader_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/max107/walreader"
)

func TestEvent(t *testing.T) {
	t.Parallel()

	event := &walreader.Event{
		Type:        walreader.Update,
		Schema:      "public",
		Table:       "test2",
		Values:      map[string]any{"id": 12.32, "product_id": "foo", "foo": "bar", "name": int32(22)},
		PrimaryKeys: []string{"id", "product_id", "name"},
	}

	require.Equal(t, "public/test2/id=12.32,product_id=foo,name=22", walreader.EventToStringKey(event))
}
