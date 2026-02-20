package analyzer

type ffprobeOutput struct {
	Streams []ffprobeStream `json:"streams"`
	Format  ffprobeFormat   `json:"format"`
}

type ffprobeStream struct {
	CodecType    string            `json:"codec_type"`
	CodecName    string            `json:"codec_name"`
	Width        int               `json:"width"`
	Height       int               `json:"height"`
	AvgFrameRate string            `json:"avg_frame_rate"`
	RFrameRate   string            `json:"r_frame_rate"`
	BitRate      string            `json:"bit_rate"`
	Tags         ffprobeStreamTags `json:"tags"`
}

type ffprobeStreamTags struct {
	VariantBitrate string `json:"variant_bitrate"`
}

type ffprobeFormat struct {
	BitRate string `json:"bit_rate"`
}
