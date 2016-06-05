// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package dataset

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func createTestDataset(name string) Dataset {
	return NewDataset(datas.NewDatabase(chunks.NewTestStore()), name)
}

func TestValidateRef(t *testing.T) {
	ds := createTestDataset("test")
	b := types.Bool(true)
	r := ds.Database().WriteValue(b)

	assert.Panics(t, func() { ds.validateRefAsCommit(r) })
	assert.Panics(t, func() { ds.validateRefAsCommit(types.NewRef(b)) })
}

func NewList(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewList(vs...)
	return ds.Database().WriteValue(v)
}

func NewMap(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewMap(vs...)
	return ds.Database().WriteValue(v)
}

func NewSet(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewSet(vs...)
	return ds.Database().WriteValue(v)
}

func pullTest(t *testing.T, topdown bool) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	// Give sink and source some initial shared context.
	sourceInitialValue := types.NewMap(
		types.NewString("first"), NewList(source),
		types.NewString("second"), NewList(source, types.Number(2)))
	sinkInitialValue := types.NewMap(
		types.NewString("first"), NewList(sink),
		types.NewString("second"), NewList(sink, types.Number(2)))

	var err error
	source, err = source.Commit(sourceInitialValue)
	assert.NoError(err)
	sink, err = sink.Commit(sinkInitialValue)
	assert.NoError(err)

	// Add some new stuff to source.
	updatedValue := sourceInitialValue.Set(
		types.NewString("third"), NewList(source, types.Number(3)))
	source, err = source.Commit(updatedValue)
	assert.NoError(err)

	// Add some more stuff, so that source isn't directly ahead of sink.
	updatedValue = updatedValue.Set(
		types.NewString("fourth"), NewList(source, types.Number(4)))
	source, err = source.Commit(updatedValue)
	assert.NoError(err)

	sink, err = sink.pull(source.Database(), types.NewRef(source.Head()), 1, topdown)
	assert.NoError(err)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullTopDown(t *testing.T) {
	pullTest(t, true)
}

func TestPullExclude(t *testing.T) {
	pullTest(t, false)
}

func pullFirstCommit(t *testing.T, topdown bool) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	sourceInitialValue := types.NewMap(
		types.NewString("first"), NewList(source),
		types.NewString("second"), NewList(source, types.Number(2)))

	NewList(sink)
	NewList(sink, types.Number(2))

	source, err := source.Commit(sourceInitialValue)
	assert.NoError(err)

	sink, err = sink.pull(source.Database(), types.NewRef(source.Head()), 1, topdown)
	assert.NoError(err)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullFirstCommitTopDown(t *testing.T) {
	pullFirstCommit(t, true)
}

func TestPullFirstCommitExclude(t *testing.T) {
	pullFirstCommit(t, false)
}

func pullDeepRef(t *testing.T, topdown bool) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	sourceInitialValue := types.NewList(
		types.NewList(NewList(source)),
		types.NewSet(NewSet(source)),
		types.NewMap(NewMap(source), NewMap(source)))

	source, err := source.Commit(sourceInitialValue)
	assert.NoError(err)

	sink, err = sink.pull(source.Database(), types.NewRef(source.Head()), 1, topdown)
	assert.NoError(err)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullDeepRefTopDown(t *testing.T) {
	pullDeepRef(t, true)
}

func TestPullDeepRefExclude(t *testing.T) {
	pullDeepRef(t, false)
}