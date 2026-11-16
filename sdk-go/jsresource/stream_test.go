// Copyright 2026 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jsresource_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/jsresource"
	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

var bothNotices = []string{subject.SuffixOversize, subject.SuffixError}

func TestEnsureStream_CreatesMissingStream(t *testing.T) {
	js := startJetStream(t)
	res, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
		Name:          "ORDERS",
		Subjects:      []string{"mongo.shop.orders"},
		Notices:       bothNotices,
		AtomicPublish: true,
		Create:        jetstream.StreamConfig{MaxAge: time.Hour, Storage: jetstream.MemoryStorage},
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if !res.Created || res.Updated {
		t.Errorf("Created, Updated = %v, %v, want true, false", res.Created, res.Updated)
	}
	cfg := streamConfig(t, js, "ORDERS")
	want := []string{"mongo.shop.orders", "mongo.shop.orders.oversize", "mongo.shop.orders.error"}
	if !slices.Equal(cfg.Subjects, want) {
		t.Errorf("Subjects = %q, want %q", cfg.Subjects, want)
	}
	if !cfg.AllowAtomicPublish || cfg.MaxAge != time.Hour || cfg.Storage != jetstream.MemoryStorage {
		t.Errorf("AllowAtomicPublish, MaxAge, Storage = %v, %v, %v; want the template's", cfg.AllowAtomicPublish, cfg.MaxAge, cfg.Storage)
	}
}

func TestEnsureStream_TemplateSubjectsUsedWhenTheyCapture(t *testing.T) {
	js := startJetStream(t)
	_, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
		Name:     "SHOP",
		Subjects: []string{"mongo.shop.orders"},
		Notices:  bothNotices,
		Create:   jetstream.StreamConfig{Subjects: []string{"mongo.shop.>"}},
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if got := streamConfig(t, js, "SHOP").Subjects; !slices.Equal(got, []string{"mongo.shop.>"}) {
		t.Errorf("Subjects = %q, want the template's", got)
	}
}

// TestEnsureStream_NeverLeavesExistingStreamAlone covers the operator-owned
// case: a stream that already captures everything is used exactly as it is.
func TestEnsureStream_NeverLeavesExistingStreamAlone(t *testing.T) {
	js := startJetStream(t)
	op := jetstream.StreamConfig{
		Name: "EVENTS", Subjects: []string{"events.>"}, Retention: jetstream.InterestPolicy,
		MaxMsgs: 100, MaxAge: time.Hour, Discard: jetstream.DiscardNew, Description: "operator",
	}
	mustCreateStream(t, js, op)
	before := streamConfig(t, js, "EVENTS")

	res, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
		Name: "EVENTS", Subjects: []string{"events.s3"}, Notices: []string{subject.SuffixOversize}, AtomicPublish: true,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if res.Created || res.Updated {
		t.Errorf("Created, Updated = %v, %v, want false, false", res.Created, res.Updated)
	}
	after := streamConfig(t, js, "EVENTS")
	if !slices.Equal(after.Subjects, before.Subjects) || after.Retention != before.Retention || after.MaxMsgs != before.MaxMsgs ||
		after.Discard != before.Discard || after.Description != before.Description || after.AllowAtomicPublish {
		t.Errorf("stream changed: before %+v, after %+v", before, after)
	}
}

func TestEnsureStream_NeverRejectsMissingSubjects(t *testing.T) {
	tests := []struct {
		name     string
		stream   []string
		subjects []string
		notices  []string
		missing  string
	}{
		{"data subject", []string{"other.>"}, []string{"events.s3"}, nil, "events.s3"},
		{"notice subject", []string{"events.s3"}, []string{"events.s3"}, []string{subject.SuffixOversize}, "events.s3.oversize"},
		{"pattern not covered by literals", []string{"events.a", "events.b"}, []string{"events.*"}, nil, "events.*"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := startJetStream(t)
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: tt.stream})
			_, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
				Name: "S", Subjects: tt.subjects, Notices: tt.notices,
			})
			if !errors.Is(err, jsresource.ErrNotCaptured) || !strings.Contains(err.Error(), tt.missing) {
				t.Fatalf("EnsureStream error = %v, want ErrNotCaptured naming %q", err, tt.missing)
			}
			if got := streamConfig(t, js, "S").Subjects; !slices.Equal(got, tt.stream) {
				t.Errorf("Subjects = %q, want them unchanged", got)
			}
		})
	}
}

func TestEnsureStream_RejectsUnpublishableStreams(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, js jetstream.JetStream)
		want  string
	}{
		{"no ack", func(t *testing.T, js jetstream.JetStream) {
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}, NoAck: true})
		}, "no_ack"},
		{"sealed", func(t *testing.T, js jetstream.JetStream) {
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
			cfg := streamConfig(t, js, "S")
			cfg.Sealed = true
			if _, err := js.UpdateStream(context.Background(), cfg); err != nil {
				t.Fatalf("seal: %v", err)
			}
		}, "sealed"},
		{"counter", func(t *testing.T, js jetstream.JetStream) {
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}, AllowMsgCounter: true})
		}, "counter"},
		{"mirror", func(t *testing.T, js jetstream.JetStream) {
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "ORIGIN", Subjects: []string{"e.>"}})
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Mirror: &jetstream.StreamSource{Name: "ORIGIN"}})
		}, "mirror"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := startJetStream(t)
			tt.setup(t, js)
			for _, policy := range []jsresource.UpdatePolicy{jsresource.UpdateNever, jsresource.UpdateAdditive} {
				_, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
					Name: "S", Subjects: []string{"e.x"}, Update: policy,
				})
				if !errors.Is(err, jsresource.ErrIncompatible) || !strings.Contains(err.Error(), tt.want) {
					t.Errorf("policy %d: EnsureStream error = %v, want ErrIncompatible naming %q", policy, err, tt.want)
				}
			}
		})
	}
}

// TestEnsureStream_AdditiveAppendsOnlyWhatIsMissing pins UpdateAdditive to
// subjects and AllowAtomicPublish: every other operator setting survives.
func TestEnsureStream_AdditiveAppendsOnlyWhatIsMissing(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{
		Name: "SHOP", Subjects: []string{"m.shop.base"}, Retention: jetstream.InterestPolicy,
		MaxMsgs: 100, MaxAge: time.Hour, Discard: jetstream.DiscardNew,
	})
	res, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
		Name: "SHOP", Subjects: []string{"m.shop.base", "m.shop.orders"}, Notices: bothNotices,
		AtomicPublish: true, Update: jsresource.UpdateAdditive,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if res.Created || !res.Updated {
		t.Errorf("Created, Updated = %v, %v, want false, true", res.Created, res.Updated)
	}
	cfg := streamConfig(t, js, "SHOP")
	want := []string{"m.shop.base", "m.shop.base.oversize", "m.shop.base.error", "m.shop.orders", "m.shop.orders.oversize", "m.shop.orders.error"}
	if !slices.Equal(cfg.Subjects, want) {
		t.Errorf("Subjects = %q, want %q", cfg.Subjects, want)
	}
	if !cfg.AllowAtomicPublish {
		t.Error("AllowAtomicPublish = false, want true")
	}
	if cfg.Retention != jetstream.InterestPolicy || cfg.MaxMsgs != 100 || cfg.MaxAge != time.Hour || cfg.Discard != jetstream.DiscardNew {
		t.Errorf("operator settings changed: %+v", cfg)
	}
	if cfg := res.Stream.CachedInfo().Config; !slices.Equal(cfg.Subjects, want) {
		t.Errorf("result Subjects = %q, want the re-read %q", cfg.Subjects, want)
	}
}

func TestEnsureStream_AdditiveSendsNoUpdateWhenNothingIsMissing(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}, AllowAtomicPublish: true})
	cj := &clobberJS{JetStream: js}
	res, err := jsresource.EnsureStream(context.Background(), cj, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e.x"}, Notices: bothNotices, AtomicPublish: true, Update: jsresource.UpdateAdditive,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if res.Updated || cj.updates != 0 {
		t.Errorf("Updated = %v after %d updates, want no update", res.Updated, cj.updates)
	}
}

// TestEnsureStream_AdditiveSurvivesALostUpdate reproduces a lost update:
// another writer's update, built from an older read, replaces this
// one. EnsureStream reads the stream back, sees its subjects gone, and
// appends them again.
func TestEnsureStream_AdditiveSurvivesALostUpdate(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "SHOP", Subjects: []string{"m.shop.base"}})
	cj := &clobberJS{JetStream: js, n: 1, clobber: func() {
		cfg := streamConfig(t, js, "SHOP")
		cfg.Subjects = []string{"m.shop.base", "m.shop.payments"}
		if _, err := js.UpdateStream(context.Background(), cfg); err != nil {
			t.Fatalf("competing update: %v", err)
		}
	}}
	_, err := jsresource.EnsureStream(context.Background(), cj, jsresource.StreamSpec{
		Name: "SHOP", Subjects: []string{"m.shop.orders"}, Update: jsresource.UpdateAdditive,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if got, want := streamConfig(t, js, "SHOP").Subjects, []string{"m.shop.base", "m.shop.payments", "m.shop.orders"}; !slices.Equal(got, want) {
		t.Errorf("Subjects = %q, want %q", got, want)
	}
	if cj.updates != 2 {
		t.Errorf("updates = %d, want 2", cj.updates)
	}
}

func TestEnsureStream_AdditiveGivesUpWhenUpdatesKeepGettingLost(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "SHOP", Subjects: []string{"m.shop.base"}})
	cj := &clobberJS{JetStream: js, n: 1 << 30, clobber: func() {
		cfg := streamConfig(t, js, "SHOP")
		cfg.Subjects = []string{"m.shop.base"}
		if _, err := js.UpdateStream(context.Background(), cfg); err != nil {
			t.Fatalf("competing update: %v", err)
		}
	}}
	res, err := jsresource.EnsureStream(context.Background(), cj, jsresource.StreamSpec{
		Name: "SHOP", Subjects: []string{"m.shop.orders"}, Update: jsresource.UpdateAdditive,
	})
	if !errors.Is(err, jsresource.ErrNotCaptured) || !strings.Contains(err.Error(), "m.shop.orders") {
		t.Fatalf("EnsureStream error = %v, want ErrNotCaptured naming m.shop.orders", err)
	}
	if cj.updates != 3 || !res.Updated {
		t.Errorf("updates = %d, Updated = %v; want 3, and true, since the stream did change", cj.updates, res.Updated)
	}
}

// TestEnsureStream_AdditiveSucceedsInTheLastRound: two updates are lost, and
// the third, the last round, sticks.
func TestEnsureStream_AdditiveSucceedsInTheLastRound(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "SHOP", Subjects: []string{"m.shop.base"}})
	cj := &clobberJS{JetStream: js, n: 2, clobber: func() {
		cfg := streamConfig(t, js, "SHOP")
		cfg.Subjects = []string{"m.shop.base"}
		if _, err := js.UpdateStream(context.Background(), cfg); err != nil {
			t.Fatalf("competing update: %v", err)
		}
	}}
	res, err := jsresource.EnsureStream(context.Background(), cj, jsresource.StreamSpec{
		Name: "SHOP", Subjects: []string{"m.shop.orders"}, Update: jsresource.UpdateAdditive,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if cj.updates != 3 || !res.Updated {
		t.Errorf("updates = %d, Updated = %v; want 3, true", cj.updates, res.Updated)
	}
	if got, want := streamConfig(t, js, "SHOP").Subjects, []string{"m.shop.base", "m.shop.orders"}; !slices.Equal(got, want) {
		t.Errorf("Subjects = %q, want %q", got, want)
	}
}

// TestEnsureStream_ReadBackFailureKeepsUpdated: the update landed, and only
// reading the stream back failed. The error comes with Updated, so the caller
// can still log that the stream changed.
func TestEnsureStream_ReadBackFailureKeepsUpdated(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.a"}})
	lost := errors.New("read-back lost")
	res, err := jsresource.EnsureStream(context.Background(), &failLaterStreamJS{JetStream: js, err: lost}, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e.b"}, Update: jsresource.UpdateAdditive,
	})
	if !errors.Is(err, lost) || !res.Updated || res.Stream != nil {
		t.Errorf("EnsureStream = %+v, %v; want Updated, no stream, and the read-back error", res, err)
	}
}

// TestEnsureStream_ReservedSubjectsMatchTheServer: EnsureStream refuses as
// reserved exactly the subjects the server refuses on a stream that
// acknowledges publishes, carve-outs included.
func TestEnsureStream_ReservedSubjectsMatchTheServer(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	for i, subj := range []string{
		">", "*", "*.orders", "*.*.x", "orders.*", "orders.>", "$KV.b.k",
		"$JS.x", "$JS.>", "$JS.*.x", "$JS.EVENT.x", "$JS.EVENT.>",
		"$JSC.x", "$NRG.x", "$SYS.x", "$SYS.ACCOUNT.x", "$SYS.ACCOUNT.>",
	} {
		name := fmt.Sprintf("R%d", i)
		_, serverErr := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subj}})
		if serverErr == nil {
			if err := js.DeleteStream(ctx, name); err != nil {
				t.Fatalf("DeleteStream(%s): %v", name, err)
			}
		}
		_, err := jsresource.EnsureStream(ctx, js, jsresource.StreamSpec{Name: name, Subjects: []string{subj}})
		if err == nil {
			if err := js.DeleteStream(ctx, name); err != nil {
				t.Fatalf("DeleteStream(%s): %v", name, err)
			}
		}
		if refused := errors.Is(err, jsresource.ErrInvalid); refused != (serverErr != nil) {
			t.Errorf("%q: EnsureStream error = %v, server create error = %v; want both or neither", subj, err, serverErr)
		}
	}
}

// TestEnsureStream_ConcurrentCreateValidatesTheWinner covers the create race:
// another instance created the stream, with a
// different config, after this one's lookup. That is not an error; the
// stream it created is validated like any other.
func TestEnsureStream_ConcurrentCreateValidatesTheWinner(t *testing.T) {
	t.Run("winner captures", func(t *testing.T) {
		js := startJetStream(t)
		mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
		res, err := jsresource.EnsureStream(context.Background(), &hiddenStreamJS{JetStream: js}, jsresource.StreamSpec{
			Name: "S", Subjects: []string{"e.x"},
		})
		if err != nil {
			t.Fatalf("EnsureStream: %v", err)
		}
		if res.Created {
			t.Error("Created = true, want false: another instance created it")
		}
	})
	t.Run("winner does not capture", func(t *testing.T) {
		js := startJetStream(t)
		mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"other"}})
		_, err := jsresource.EnsureStream(context.Background(), &hiddenStreamJS{JetStream: js}, jsresource.StreamSpec{
			Name: "S", Subjects: []string{"e.x"},
		})
		if !errors.Is(err, jsresource.ErrNotCaptured) {
			t.Fatalf("EnsureStream error = %v, want ErrNotCaptured", err)
		}
	})
}

func TestEnsureStream_RejectsInvalidInput(t *testing.T) {
	// The rows share one server: each is rejected before any request is
	// sent. A row that reached the server would leak state into the next.
	js := startJetStream(t)
	tests := []struct {
		name string
		s    jsresource.StreamSpec
		want string // in the error, so a row can't pass on another rule
	}{
		{"bad name", jsresource.StreamSpec{Name: "a.b", Subjects: []string{"e"}}, `stream name "a.b"`},
		{"slash in name", jsresource.StreamSpec{Name: "a/b", Subjects: []string{"e"}}, `stream name "a/b"`},
		{"no subjects", jsresource.StreamSpec{Name: "S"}, "no subjects"},
		{"bad subject", jsresource.StreamSpec{Name: "S", Subjects: []string{"e..x"}}, `subject "e..x" is not`},
		{"bad suffix", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Notices: []string{"a.b"}}, `notice suffix "a.b"`},
		{"template name differs", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Create: jetstream.StreamConfig{Name: "T"}}, "Create.Name"},
		{"template subjects miss one", jsresource.StreamSpec{Name: "S", Subjects: []string{"e.x"}, Notices: bothNotices, Create: jetstream.StreamConfig{Subjects: []string{"e.x"}}}, "do not capture"},
		{"unknown policy", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Update: 7}, "unknown update policy"},
		{"no-ack template", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Create: jetstream.StreamConfig{NoAck: true}}, "sets NoAck"},
		{"sealed template", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Create: jetstream.StreamConfig{Sealed: true}}, "sets Sealed"},
		{"mirror template", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Create: jetstream.StreamConfig{Mirror: &jetstream.StreamSource{Name: "O"}}}, "sets Mirror"},
		{"counter template", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Create: jetstream.StreamConfig{AllowMsgCounter: true}}, "sets AllowMsgCounter"},
		{"template subject invalid", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Create: jetstream.StreamConfig{Subjects: []string{"e", "e..x"}}}, `Create subject "e..x"`},
		{"partially overlapping subjects", jsresource.StreamSpec{Name: "S", Subjects: []string{"x.a.*", "x.*.b"}}, "overlap: list one pattern"},
		{"unknown notice suffix", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Notices: []string{"oversized"}}, `notice suffix "oversized"`},
		{"subject only a no-ack stream may capture", jsresource.StreamSpec{Name: "S", Subjects: []string{">"}}, `">" overlaps the server's reserved`},
		{"subject overlapping the JetStream API", jsresource.StreamSpec{Name: "S", Subjects: []string{"*.orders"}}, "reserved $JS.>"},
		{"template subject overlapping the system account", jsresource.StreamSpec{Name: "S", Subjects: []string{"e"}, Create: jetstream.StreamConfig{Subjects: []string{"e", "$SYS.x"}}}, "reserved $SYS.>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := jsresource.EnsureStream(context.Background(), js, tt.s); !errors.Is(err, jsresource.ErrInvalid) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("EnsureStream error = %v, want ErrInvalid saying %q", err, tt.want)
			}
			if _, err := js.Stream(context.Background(), "S"); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Errorf("stream S exists after rejected input (err %v)", err)
			}
		})
	}
}

// TestEnsureStream_CreatesMinimalSubjects: a required subject that another
// covers is dropped, and an exact duplicate is kept once, so the list can be
// one stream's subjects.
func TestEnsureStream_CreatesMinimalSubjects(t *testing.T) {
	for _, tt := range []struct {
		subjects, want []string
	}{
		{[]string{"a.>", "a.b"}, []string{"a.>"}},
		{[]string{"a.b", "a.>"}, []string{"a.>"}},
		{[]string{"a.b", "a.b"}, []string{"a.b"}},
	} {
		js := startJetStream(t)
		if _, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{Name: "S", Subjects: tt.subjects}); err != nil {
			t.Fatalf("EnsureStream(%q): %v", tt.subjects, err)
		}
		if got := streamConfig(t, js, "S").Subjects; !slices.Equal(got, tt.want) {
			t.Errorf("Subjects for %q = %q, want %q", tt.subjects, got, tt.want)
		}
	}
}

func TestEnsureStream_PatternSubjectWithNotices(t *testing.T) {
	js := startJetStream(t)
	if _, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"events.*"}, Notices: bothNotices,
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	want := []string{"events.*", "events.*.oversize", "events.*.error"}
	if got := streamConfig(t, js, "S").Subjects; !slices.Equal(got, want) {
		t.Errorf("Subjects = %q, want %q", got, want)
	}
}

// TestEnsureStream_AdditiveRefusesAnOverlappingSubject: a required subject
// that overlaps one the stream has can't be appended (the server rejects
// overlapping subjects in one stream). EnsureStream says so, naming both,
// without sending the update.
func TestEnsureStream_AdditiveRefusesAnOverlappingSubject(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"events.a"}})
	cj := &clobberJS{JetStream: js}
	_, err := jsresource.EnsureStream(context.Background(), cj, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"events.>"}, Update: jsresource.UpdateAdditive,
	})
	if !errors.Is(err, jsresource.ErrNotCaptured) || !strings.Contains(err.Error(), "events.a") || !strings.Contains(err.Error(), "events.>") {
		t.Fatalf("EnsureStream error = %v, want ErrNotCaptured naming events.> and events.a", err)
	}
	if cj.updates != 0 {
		t.Errorf("updates = %d, want none", cj.updates)
	}
}

// TestEnsureStream_DeniedCreateAlsoReportsNotFound: a credential without
// STREAM.CREATE meets a missing stream (often a mistyped name). The error
// matches both ErrPermission and jetstream.ErrStreamNotFound.
func TestEnsureStream_DeniedCreateAlsoReportsNotFound(t *testing.T) {
	_, limited := startLimited(t, "$JS.API.STREAM.CREATE.>")
	_, err := jsresource.EnsureStream(context.Background(), limited, jsresource.StreamSpec{
		Name: "TYPO", Subjects: []string{"e"}, Timeout: time.Second,
	})
	if !errors.Is(err, jsresource.ErrPermission) || !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("EnsureStream error = %v, want ErrPermission and ErrStreamNotFound", err)
	}
}

// TestEnsureStream_NamesTheDeniedPermission: a denied JS API request only
// times out, and the connection's last error names the subject.
func TestEnsureStream_NamesTheDeniedPermission(t *testing.T) {
	_, limited := startLimited(t, "$JS.API.STREAM.INFO.>")
	_, err := jsresource.EnsureStream(context.Background(), limited, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e"}, Timeout: time.Second,
	})
	if !errors.Is(err, jsresource.ErrPermission) || !strings.Contains(err.Error(), "$JS.API.STREAM.INFO.S") {
		t.Fatalf("EnsureStream error = %v, want ErrPermission naming $JS.API.STREAM.INFO.S", err)
	}
	if !errors.Is(err, nats.ErrPermissionViolation) {
		t.Errorf("EnsureStream error = %v, want it to wrap nats.ErrPermissionViolation", err)
	}
}

// TestEnsureStream_AtomicIsBestEffort covers mongodb's least-privilege case:
// the credential may not update the stream, and only the
// capability is missing. EnsureStream returns the stream with the
// capability off, in one per-request timeout, instead of failing.
func TestEnsureStream_AtomicIsBestEffort(t *testing.T) {
	admin, limited := startLimited(t, "$JS.API.STREAM.UPDATE.>")
	mustCreateStream(t, admin, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
	start := time.Now()
	res, err := jsresource.EnsureStream(context.Background(), limited, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e.x"}, AtomicPublish: true, Update: jsresource.UpdateAdditive,
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if res.Updated || res.Stream.CachedInfo().Config.AllowAtomicPublish {
		t.Errorf("Updated = %v, AllowAtomicPublish = %v, want both false", res.Updated, res.Stream.CachedInfo().Config.AllowAtomicPublish)
	}
	if !errors.Is(res.AtomicErr, jsresource.ErrPermission) {
		t.Errorf("AtomicErr = %v, want ErrPermission", res.AtomicErr)
	}
	if elapsed := time.Since(start); elapsed > 1900*time.Millisecond {
		t.Errorf("EnsureStream took %v, want about one request timeout", elapsed)
	}

	// Missing subjects are not best-effort, and the denied update is not
	// sent a second time without the capability.
	start = time.Now()
	_, err = jsresource.EnsureStream(context.Background(), limited, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"f.x"}, AtomicPublish: true, Update: jsresource.UpdateAdditive,
		Timeout: time.Second,
	})
	if !errors.Is(err, jsresource.ErrPermission) || !strings.Contains(err.Error(), "$JS.API.STREAM.UPDATE.S") {
		t.Fatalf("EnsureStream error = %v, want ErrPermission naming $JS.API.STREAM.UPDATE.S", err)
	}
	if elapsed := time.Since(start); elapsed > 1900*time.Millisecond {
		t.Errorf("EnsureStream took %v, want about one request timeout", elapsed)
	}
}

// TestEnsureStream_NoAtomicUpdateBeforeServer212: an older server accepts
// AllowAtomicPublish and drops it, so EnsureStream doesn't ask: no update,
// and nothing to report in AtomicErr.
func TestEnsureStream_NoAtomicUpdateBeforeServer212(t *testing.T) {
	defer jsresource.SetServerVersion("2.11.9")()
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
	cj := &clobberJS{JetStream: js}
	res, err := jsresource.EnsureStream(context.Background(), cj, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e.x"}, AtomicPublish: true, Update: jsresource.UpdateAdditive,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if res.Updated || cj.updates != 0 || res.AtomicErr != nil {
		t.Errorf("Updated = %v after %d updates, AtomicErr = %v; want no update and no error", res.Updated, cj.updates, res.AtomicErr)
	}
}
