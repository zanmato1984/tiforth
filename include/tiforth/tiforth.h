// Copyright 2026 TiForth Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#define TIFORTH_VERSION_MAJOR 0
#define TIFORTH_VERSION_MINOR 1
#define TIFORTH_VERSION_PATCH 0

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/traits.h"
#include "tiforth/spill.h"
#include "tiforth/type_metadata.h"

#include "tiforth/operators/filter.h"
#include "tiforth/operators/arrow_compute_agg.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/operators/pass_through.h"
#include "tiforth/operators/projection.h"
#include "tiforth/operators/sort.h"
