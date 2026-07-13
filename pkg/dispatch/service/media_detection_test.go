package service

import (
	"reflect"
	"testing"
)

func TestMediaFileType(t *testing.T) {
	cases := map[string]string{
		"https://x/v.mp4":            "video",
		"https://x/v.MP4":            "video",
		"https://x/v.mp4?token=abc":  "video", // querystring ignored
		"https://x/v.mov#t=10":       "video",
		"https://x/pic.jpg":          "image",
		"https://x/pic.png":          "image",
		"https://x/song.mp3":         "audio",
		"https://x/doc.pdf":          "file", // document -> file
		"https://x/sheet.xlsx":       "file",
		"https://site.com/page":      "",     // no extension
		"https://site.com/page.html": "",     // not media
		"https://x/no-dot/segment":   "",
	}
	for url, want := range cases {
		if got := mediaFileType(url); got != want {
			t.Errorf("mediaFileType(%q) = %q, want %q", url, got, want)
		}
	}
}

func TestExtractMediaURLs(t *testing.T) {
	t.Run("text with trailing media url", func(t *testing.T) {
		text, atts := extractMediaURLs("Assiste aí https://pedrofelixtreinador.com.br/x/VLS_Atleta.mp4")
		if text != "Assiste aí" {
			t.Errorf("text = %q, want %q", text, "Assiste aí")
		}
		want := []postbackAttachment{{URL: "https://pedrofelixtreinador.com.br/x/VLS_Atleta.mp4", FileType: "video"}}
		if !reflect.DeepEqual(atts, want) {
			t.Errorf("atts = %+v, want %+v", atts, want)
		}
	})

	t.Run("media only (residual empty)", func(t *testing.T) {
		text, atts := extractMediaURLs("https://x/pic.jpg")
		if text != "" {
			t.Errorf("text = %q, want empty", text)
		}
		if len(atts) != 1 || atts[0].FileType != "image" {
			t.Errorf("atts = %+v, want one image", atts)
		}
	})

	t.Run("non-media url stays in text", func(t *testing.T) {
		text, atts := extractMediaURLs("veja em https://site.com/pagina")
		if text != "veja em https://site.com/pagina" {
			t.Errorf("text = %q, should keep non-media url", text)
		}
		if len(atts) != 0 {
			t.Errorf("atts = %+v, want none", atts)
		}
	})

	t.Run("no urls", func(t *testing.T) {
		text, atts := extractMediaURLs("plain text only")
		if text != "plain text only" || len(atts) != 0 {
			t.Errorf("got text=%q atts=%+v", text, atts)
		}
	})

	t.Run("trailing punctuation trimmed", func(t *testing.T) {
		_, atts := extractMediaURLs("olha (https://x/v.mp4).")
		if len(atts) != 1 || atts[0].URL != "https://x/v.mp4" {
			t.Errorf("atts = %+v, want trimmed url", atts)
		}
	})
}
