package config

import (
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtectedCredentialDarwinACLs(t *testing.T) {
	for _, scope := range []string{"file", "parent", "ancestor"} {
		t.Run(scope, func(t *testing.T) {
			path := credentialFixture(t, "fixture")
			target := path
			switch scope {
			case "parent":
				target = filepath.Dir(path)
			case "ancestor":
				target = filepath.Dir(path)
				private := filepath.Join(target, "private")
				require.NoError(t, os.Mkdir(private, 0o700))
				path = filepath.Join(private, "key")
				require.NoError(t, os.WriteFile(path, []byte("fixture"), 0o600))
			}
			require.NoError(t, exec.CommandContext(t.Context(), "/bin/chmod", "+a", "everyone allow read,search", target).Run())
			_, err := readProtectedCredential(path)
			require.Error(t, err)
			require.NoError(t, exec.CommandContext(t.Context(), "/bin/chmod", "-N", target).Run())
			require.NoError(t, exec.CommandContext(t.Context(), "/bin/chmod", "+a", "everyone deny delete", target).Run())
			value, err := readProtectedCredential(path)
			require.NoError(t, err)
			require.Equal(t, "fixture", value)
			require.NoError(t, exec.CommandContext(t.Context(), "/bin/chmod", "-N", target).Run())
		})
	}
}

func TestDarwinCredentialACLParserBounds(t *testing.T) {
	put := binary.LittleEndian.PutUint32
	none := make([]byte, 12)
	put(none[:4], 12)
	put(none[4:8], 8)
	require.True(t, credentialDarwinACLProtected(none))
	deny := make([]byte, 80)
	put(deny[:4], 80)
	put(deny[4:8], 8)
	put(deny[8:12], 68)
	put(deny[12:16], 0x012cc16d)
	put(deny[48:52], 1)
	put(deny[72:76], 2)
	require.True(t, credentialDarwinACLProtected(deny))
	for _, mutation := range []struct{ offset, value uint32 }{
		{0, 4097}, {4, 0xffffffff}, {8, 0xffffffff}, {12, 0}, {48, 129}, {72, 1}, {72, 3},
	} {
		bad := append([]byte(nil), deny...)
		put(bad[mutation.offset:mutation.offset+4], mutation.value)
		require.False(t, credentialDarwinACLProtected(bad))
	}
	for n := range 80 {
		require.False(t, credentialDarwinACLProtected(deny[:n]))
	}
}
