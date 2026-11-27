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

package cfgtags_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/synadia-io/connectors-sdk/sdk-go/internal/cfgtags"
)

func TestParseTags(t *testing.T) {
	type fixture struct {
		Field string `json:"field" schema:"required,default=hello,enum=a|b|hello,description=a test field" display:"label=Field,group=G,order=2,placeholder=type here,hidden,cp-type=bucket"`
	}
	f, ok := reflect.TypeOf(fixture{}).FieldByName("Field")
	if !ok {
		t.Fatal("fixture.Field not found")
	}

	pt, err := cfgtags.ParseTags(f)
	if err != nil {
		t.Fatalf("ParseTags() error = %v", err)
	}
	if !pt.Required {
		t.Error("Required = false, want true")
	}
	if !pt.HasDefault || pt.Default != "hello" {
		t.Errorf("Default = (%v, %q), want (true, %q)", pt.HasDefault, pt.Default, "hello")
	}
	if want := []string{"a", "b", "hello"}; !reflect.DeepEqual(pt.Enum, want) {
		t.Errorf("Enum = %v, want %v", pt.Enum, want)
	}
	if pt.Description != "a test field" {
		t.Errorf("Description = %q, want %q", pt.Description, "a test field")
	}
	if pt.Display.Label != "Field" || pt.Display.Group != "G" {
		t.Errorf("Display.Label/Group = %q/%q, want Field/G", pt.Display.Label, pt.Display.Group)
	}
	if pt.Display.Order == nil || *pt.Display.Order != 2 {
		t.Errorf("Display.Order = %v, want 2", pt.Display.Order)
	}
	if pt.Display.Placeholder != "type here" {
		t.Errorf("Display.Placeholder = %q, want %q", pt.Display.Placeholder, "type here")
	}
	if !pt.Display.Hidden {
		t.Error("Display.Hidden = false, want true")
	}
	if pt.Display.CPType != "bucket" {
		t.Errorf("Display.CPType = %q, want bucket", pt.Display.CPType)
	}
}

func TestParseTags_VisibleWhenMultipleConditions(t *testing.T) {
	type fixture struct {
		Field string `json:"field" display:"visible_when=a:1|b:2"`
	}
	f, _ := reflect.TypeOf(fixture{}).FieldByName("Field")
	pt, err := cfgtags.ParseTags(f)
	if err != nil {
		t.Fatalf("ParseTags() error = %v", err)
	}
	want := map[string]string{"a": "1", "b": "2"}
	if !reflect.DeepEqual(pt.Display.VisibleWhen, want) {
		t.Errorf("VisibleWhen = %v, want %v", pt.Display.VisibleWhen, want)
	}
}

func TestParseTags_Errors(t *testing.T) {
	tests := []struct {
		name        string
		schemaTag   string
		displayTag  string
		wantErrText string
	}{
		{
			name:        "secret and secret_parts conflict",
			displayTag:  "secret,secret_parts=userinfo",
			wantErrText: "secret_parts",
		},
		{
			name:        "unknown display key",
			displayTag:  "multiline=true",
			wantErrText: "multiline",
		},
		{
			name:        "unknown schema key",
			schemaTag:   "minimum=1",
			wantErrText: "minimum",
		},
		{
			name:        "non-integer order",
			displayTag:  "order=not-a-number",
			wantErrText: "order",
		},
		{
			// A default is never scanned by the D-4 guard, so a reference
			// there would reach the connector as its credential.
			name:        "secret reference as default",
			schemaTag:   "default=secret://db/pass",
			wantErrText: "secret://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := reflect.StructField{
				Name: "Field",
				Tag:  reflect.StructTag(`json:"field" schema:"` + tt.schemaTag + `" display:"` + tt.displayTag + `"`),
			}
			_, err := cfgtags.ParseTags(field)
			if err == nil {
				t.Fatalf("ParseTags() error = nil, want error mentioning %q", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Errorf("ParseTags() error = %v, want it to mention %q", err, tt.wantErrText)
			}
		})
	}
}

// TestXDisplay_IsEmpty pins the rule the generator omits an "x-display" key
// by: only a hint with no field set at all is empty.
func TestXDisplay_IsEmpty(t *testing.T) {
	if !(cfgtags.XDisplay{}).IsEmpty() {
		t.Error("XDisplay{}.IsEmpty() = false, want true")
	}
	zero := 0
	for name, d := range map[string]cfgtags.XDisplay{
		"label":        {Label: "L"},
		"order zero":   {Order: &zero},
		"hidden":       {Hidden: true},
		"secret_parts": {SecretParts: []string{"userinfo"}},
		"visible_when": {VisibleWhen: map[string]string{"a": "1"}},
		"cp-type":      {CPType: "stream"},
		"code":         {Code: &cfgtags.CodeHint{Language: "bloblang"}},
	} {
		if d.IsEmpty() {
			t.Errorf("%s: IsEmpty() = true, want false", name)
		}
	}
}

// TestJSONFieldName pins the property-name rule the generator and
// sdk-go/config's loader share: the schema a connector advertises and the
// document the loader reads have to key off the same names, so this is a
// contract between the two packages rather than an internal detail of
// either.
func TestJSONFieldName(t *testing.T) {
	tests := []struct {
		name     string
		field    reflect.StructField
		wantName string
		wantSkip bool
	}{
		{
			name:     "explicit tag name",
			field:    reflect.StructField{Name: "MongoURI", Tag: `json:"mongo_uri"`},
			wantName: "mongo_uri",
		},
		{
			name:     "tag name with options",
			field:    reflect.StructField{Name: "MongoURI", Tag: `json:"mongo_uri,omitempty"`},
			wantName: "mongo_uri",
		},
		{
			name:     "no json tag falls back to the Go field name",
			field:    reflect.StructField{Name: "MongoURI"},
			wantName: "MongoURI",
		},
		{
			name:     "options only falls back to the Go field name",
			field:    reflect.StructField{Name: "MongoURI", Tag: `json:",omitempty"`},
			wantName: "MongoURI",
		},
		{
			name:     "bare dash skips the field",
			field:    reflect.StructField{Name: "Internal", Tag: `json:"-"`},
			wantSkip: true,
		},
		{
			name:     "dash with options is a field literally named -",
			field:    reflect.StructField{Name: "Dash", Tag: `json:"-,"`},
			wantName: "-",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, skip := cfgtags.JSONFieldName(tt.field)
			if skip != tt.wantSkip {
				t.Fatalf("JSONFieldName() skip = %v, want %v", skip, tt.wantSkip)
			}
			if !skip && name != tt.wantName {
				t.Errorf("JSONFieldName() name = %q, want %q", name, tt.wantName)
			}
		})
	}
}
