package main

/*
#include <duckdb_extension.h>
#include <stdlib.h>
*/
import "C"

import (
	"os"
	"unsafe"

	duckdb "github.com/duckdb/duckdb-go-bindings"
	"github.com/mlafeldt/quack-go/duckdbext"
)

//export initExtension
func initExtension(conn C.duckdb_connection, info C.duckdb_extension_info, access *C.struct_duckdb_extension_access) C.bool {
	mode := os.Getenv("QUACK_LLM_MODE")

	dispatcher = nil
	singleClient = nil
	fusedDispatcher = nil

	switch mode {
	case "single":
		c, err := NewAnthropicSingleClientFromEnv()
		if err != nil {
			duckdbext.SetExtensionError(
				duckdbext.ExtensionAccess{Ptr: unsafe.Pointer(access)},
				duckdbext.ExtensionInfo{Ptr: unsafe.Pointer(info)},
				"Failed to init Anthropic single client: "+err.Error(),
			)
			return C.bool(false)
		}
		singleClient = c

	case "fused":
		c, err := NewAnthropicSingleClientFromEnv()
		if err != nil {
			duckdbext.SetExtensionError(
				duckdbext.ExtensionAccess{Ptr: unsafe.Pointer(access)},
				duckdbext.ExtensionInfo{Ptr: unsafe.Pointer(info)},
				"Failed to init Anthropic single client (for fused mode): "+err.Error(),
			)
			return C.bool(false)
		}

		fusedDispatcher = NewFusedDispatcher(c, ";")

	default: // "batch"
		c, err := NewAnthropicBatchClientFromEnv()
		if err != nil {
			duckdbext.SetExtensionError(
				duckdbext.ExtensionAccess{Ptr: unsafe.Pointer(access)},
				duckdbext.ExtensionInfo{Ptr: unsafe.Pointer(info)},
				"Failed to init Anthropic batch client: "+err.Error(),
			)
			return C.bool(false)
		}
		dispatcher = NewLLMDispatcher(c)
	}

	if err := duckdbext.RegisterScalarFunction(
		duckdb.Connection{Ptr: unsafe.Pointer(conn)},
		"ai_llm",
		[]duckdb.Type{duckdb.TypeVarchar, duckdb.TypeVarchar},
		duckdb.TypeVarchar,
		aiLLM,
	); err != nil {
		duckdbext.SetExtensionError(
			duckdbext.ExtensionAccess{Ptr: unsafe.Pointer(access)},
			duckdbext.ExtensionInfo{Ptr: unsafe.Pointer(info)},
			"Failed to register ai_llm: "+err.Error(),
		)
		return C.bool(false)
	}

	return C.bool(true)
}
