package rtmp

import (
	"testing"

	"github.com/bluenviron/gortmplib"
	"github.com/stretchr/testify/require"
)

func TestToStreamNoSupportedCodecs(t *testing.T) {
	r := &gortmplib.Reader{}

	_, _, err := ToStream(r, nil, 0)
	require.Equal(t, errNoSupportedCodecsTo, err)
}

// this is impossible to test since currently we support all RTMP tracks.
// func TestToStreamSkipUnsupportedTracks(t *testing.T)
