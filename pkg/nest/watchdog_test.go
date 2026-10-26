package nest

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

func TestVideoBytesIgnoresAudio(t *testing.T) {
	video := &core.Receiver{Node: core.Node{Codec: &core.Codec{Name: core.CodecH264}}, Bytes: 1000}
	audio := &core.Receiver{Node: core.Node{Codec: &core.Codec{Name: core.CodecOpus}}, Bytes: 500}
	receivers := []*core.Receiver{video, audio}

	before := videoBytes(receivers, 1500)
	audio.Bytes += 9000 // audio keeps flowing, video is frozen
	require.Equal(t, before, videoBytes(receivers, 10500))

	video.Bytes += 10
	require.NotEqual(t, before, videoBytes(receivers, 10510))
}

func TestVideoBytesFallsBackToTotal(t *testing.T) {
	audio := &core.Receiver{Node: core.Node{Codec: &core.Codec{Name: core.CodecOpus}}, Bytes: 500}
	require.Equal(t, 700, videoBytes([]*core.Receiver{audio}, 700))
	require.Equal(t, 42, videoBytes(nil, 42))
}
