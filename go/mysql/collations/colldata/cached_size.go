/*
Copyright 2025 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// Code generated by Sizegen. DO NOT EDIT.

package colldata

import hack "vitess.io/vitess/go/hack"

type cachedObject interface {
	CachedSize(alloc bool) int64
}

func (cached *eightbitWildcard) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(32)
	}
	// field sort *[256]byte
	if cached.sort != nil {
		size += hack.RuntimeAllocSize(int64(cap(*cached.sort)))
	}
	// field pattern []int16
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.pattern)) * int64(2))
	}
	return size
}
func (cached *fastMatcher) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field pattern []byte
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.pattern)))
	}
	return size
}
func (cached *unicodeWildcard) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field charset vitess.io/vitess/go/mysql/collations/charset.Charset
	if cc, ok := cached.charset.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	// field pattern []rune
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.pattern)) * int64(4))
	}
	return size
}
