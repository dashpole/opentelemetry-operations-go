// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integrationtest

import (
	"context"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-go/extension/googleclientauthextension"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

func hostWithGoogleAuth(project string) (*fakeHost, error) {
	extension, err := googleclientauthextension.NewFactory().CreateExtension(
		context.Background(),
		extension.CreateSettings{},
		&googleclientauthextension.Config{Project: project},
	)
	return &fakeHost{
		map[component.ID]component.Component{
			component.NewID("googleclientauth"): extension,
		},
	}, err
}

type fakeHost struct {
	extensions map[component.ID]component.Component
}

func (f *fakeHost) ReportFatalError(err error) {
}

func (f *fakeHost) GetFactory(kind component.Kind, componentType component.Type) component.Factory {
	return nil
}

func (f *fakeHost) GetExtensions() map[component.ID]component.Component {
	return f.extensions
}

func (f *fakeHost) GetExporters() map[component.DataType]map[component.ID]component.Component {
	return nil
}
