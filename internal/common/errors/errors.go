// Package errors provides a standard error handling mechanism for the project.
package errors

import (
	"fmt"
	"strings"
)

// ErrorCode represents the type of error that occurred.
type ErrorCode int

// Error represents a structured error with context information.
type Error struct {
	// Code is the error code
	Code ErrorCode
	// Message is the user-facing error message
	Message string
	// InternalError is the underlying error (if any)
	InternalError error
	// Context contains additional contextual information about the error
	Context map[string]interface{}
}

// Error implements the error interface.
func (e *Error) Error() string {
	msg := fmt.Sprintf("[%d] %s", e.Code, e.Message)
	if e.InternalError != nil {
		msg += ": " + e.InternalError.Error()
	}

	if len(e.Context) > 0 {
		contextStrings := make([]string, 0, len(e.Context))
		for k, v := range e.Context {
			contextStrings = append(contextStrings, fmt.Sprintf("%s=%v", k, v))
		}
		msg += " [" + strings.Join(contextStrings, ", ") + "]"
	}

	return msg
}

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error {
	return e.InternalError
}

// Is implements the errors.Is interface for compatibility with Go 1.13+ error checking.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// General error codes
const (
	// ErrUnknown represents an unknown error
	ErrUnknown ErrorCode = 1000
	// ErrBadRequest represents an invalid request error
	ErrBadRequest ErrorCode = 1001
	// ErrInternal represents an internal server error
	ErrInternal ErrorCode = 1002
	// ErrNotFound represents a resource not found error
	ErrNotFound ErrorCode = 1003
	// ErrTimeout represents a timeout error
	ErrTimeout ErrorCode = 1004
	// ErrPermissionDenied represents a permission denied error
	ErrPermissionDenied ErrorCode = 1005
	// ErrUnauthenticated represents an unauthenticated error
	ErrUnauthenticated ErrorCode = 1006
	// ErrResourceExhausted represents a resource exhausted error
	ErrResourceExhausted ErrorCode = 1007
	// ErrCancelled represents a cancelled operation error
	ErrCancelled ErrorCode = 1008
	// ErrAlreadyExists represents a resource already exists error
	ErrAlreadyExists ErrorCode = 1009
	// ErrUnavailable represents a service unavailable error
	ErrUnavailable ErrorCode = 1010
)

// StarRocks related error codes
const (
	// ErrStarRocksConnection represents a connection error to StarRocks
	ErrStarRocksConnection ErrorCode = 2000
	// ErrTabletUnavailable represents an unavailable tablet error
	ErrTabletUnavailable ErrorCode = 2001
	// ErrBEDown represents a backend down error
	ErrBEDown ErrorCode = 2002
	// ErrFEDown represents a frontend down error
	ErrFEDown ErrorCode = 2003
	// ErrTableNotFound represents a table not found error
	ErrTableNotFound ErrorCode = 2004
	// ErrDatabaseNotFound represents a database not found error
	ErrDatabaseNotFound ErrorCode = 2005
	// ErrInvalidSQL represents an invalid SQL error
	ErrInvalidSQL ErrorCode = 2006
	// ErrTableSchema represents a table schema error
	ErrTableSchema ErrorCode = 2007
	// ErrPartitionUnavailable represents an unavailable partition error
	ErrPartitionUnavailable ErrorCode = 2008
	// ErrReplicaInconsistent represents an inconsistent replica error
	ErrReplicaInconsistent ErrorCode = 2009
	// ErrStarRocksTimeout represents a StarRocks operation timeout
	ErrStarRocksTimeout ErrorCode = 2010
)

// Query processing error codes
const (
	// ErrQueryTooComplex represents a query too complex error
	ErrQueryTooComplex ErrorCode = 3000
	// ErrPruningFailed represents a query pruning failure
	ErrPruningFailed ErrorCode = 3001
	// ErrQueryTimeout represents a query timeout error
	ErrQueryTimeout ErrorCode = 3002
	// ErrQuerySyntax represents a query syntax error
	ErrQuerySyntax ErrorCode = 3003
	// ErrQueryExecution represents a query execution error
	ErrQueryExecution ErrorCode = 3004
	// ErrQueryPlan represents a query planning error
	ErrQueryPlan ErrorCode = 3005
	// ErrQueryCancelled represents a cancelled query error
	ErrQueryCancelled ErrorCode = 3006
	// ErrQueryTooLarge represents a query result too large error
	ErrQueryTooLarge ErrorCode = 3007
	// ErrQueryLimit represents a query limit exceeded error
	ErrQueryLimit ErrorCode = 3008
	// ErrQueryParser represents a query parsing error
	ErrQueryParser ErrorCode = 3009
	// ErrQueryOptimization represents a query optimization error
	ErrQueryOptimization ErrorCode = 3010
)

// Write processing error codes
const (
	// ErrWriteBufferFull represents a write buffer full error
	ErrWriteBufferFull ErrorCode = 4000
	// ErrWriteTimeout represents a write timeout error
	ErrWriteTimeout ErrorCode = 4001
	// ErrWriteFailure represents a write failure error
	ErrWriteFailure ErrorCode = 4002
	// ErrWriteValidation represents a write validation error
	ErrWriteValidation ErrorCode = 4003
	// ErrBatchProcessing represents a batch processing error
	ErrBatchProcessing ErrorCode = 4004
	// ErrFlushFailed represents a flush failure error
	ErrFlushFailed ErrorCode = 4005
	// ErrWriteTooLarge represents a write too large error
	ErrWriteTooLarge ErrorCode = 4006
	// ErrRowFormatInvalid represents an invalid row format error
	ErrRowFormatInvalid ErrorCode = 4007
	// ErrWriteRejected represents a rejected write error
	ErrWriteRejected ErrorCode = 4008
	// ErrWriteQuota represents a write quota exceeded error
	ErrWriteQuota ErrorCode = 4009
	// ErrWriteThrottled represents a throttled write error
	ErrWriteThrottled ErrorCode = 4010
)

// Data format conversion error codes
const (
	// ErrInvalidFormat represents an invalid format error
	ErrInvalidFormat ErrorCode = 5000
	// ErrSchemaNotFound represents a schema not found error
	ErrSchemaNotFound ErrorCode = 5001
	// ErrSchemaIncompatible represents an incompatible schema error
	ErrSchemaIncompatible ErrorCode = 5002
	// ErrDataConversion represents a data conversion error
	ErrDataConversion ErrorCode = 5003
	// ErrInvalidAvro represents an invalid Avro format error
	ErrInvalidAvro ErrorCode = 5004
	// ErrInvalidJSON represents an invalid JSON format error
	ErrInvalidJSON ErrorCode = 5005
	// ErrInvalidCSV represents an invalid CSV format error
	ErrInvalidCSV ErrorCode = 5006
	// ErrFieldTypeMismatch represents a field type mismatch error
	ErrFieldTypeMismatch ErrorCode = 5007
	// ErrRequiredFieldMissing represents a missing required field error
	ErrRequiredFieldMissing ErrorCode = 5008
	// ErrSchemaEvolution represents a schema evolution error
	ErrSchemaEvolution ErrorCode = 5009
	// ErrInvalidEncoding represents an invalid encoding error
	ErrInvalidEncoding ErrorCode = 5010
)

// NewError creates a new Error with the given code and message.
func NewError(code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Context: make(map[string]interface{}),
	}
}

// WrapError wraps an existing error with a new Error.
func WrapError(code ErrorCode, message string, err error) *Error {
	return &Error{
		Code:          code,
		Message:       message,
		InternalError: err,
		Context:       make(map[string]interface{}),
	}
}

// WithContext adds context information to an Error.
func WithContext(err *Error, key string, value interface{}) *Error {
	if err.Context == nil {
		err.Context = make(map[string]interface{})
	}
	err.Context[key] = value
	return err
}

// WithContextMap adds multiple context key/values to an Error.
func WithContextMap(err *Error, ctx map[string]interface{}) *Error {
	if err.Context == nil {
		err.Context = make(map[string]interface{})
	}
	for k, v := range ctx {
		err.Context[k] = v
	}
	return err
}

// NewBadRequestError creates a new bad request error.
func NewBadRequestError(message string) *Error {
	return NewError(ErrBadRequest, message)
}

// NewInternalError creates a new internal server error.
func NewInternalError(message string) *Error {
	return NewError(ErrInternal, message)
}

// NewTimeoutError creates a new timeout error.
func NewTimeoutError(message string) *Error {
	return NewError(ErrTimeout, message)
}

// NewNotFoundError creates a new not found error.
func NewNotFoundError(message string) *Error {
	return NewError(ErrNotFound, message)
}

// NewTabletUnavailableError creates a new tablet unavailable error.
func NewTabletUnavailableError(tabletID string) *Error {
	err := NewError(ErrTabletUnavailable, "Tablet is unavailable")
	return WithContext(err, "tabletID", tabletID)
}

// NewBEDownError creates a new backend down error.
func NewBEDownError(beID string) *Error {
	err := NewError(ErrBEDown, "Backend node is down")
	return WithContext(err, "beID", beID)
}

// NewQueryTooComplexError creates a new query too complex error.
func NewQueryTooComplexError(complexity int) *Error {
	err := NewError(ErrQueryTooComplex, "Query is too complex to process")
	return WithContext(err, "complexity", complexity)
}

// NewWriteBufferFullError creates a new write buffer full error.
func NewWriteBufferFullError(bufferSize int) *Error {
	err := NewError(ErrWriteBufferFull, "Write buffer is full")
	return WithContext(err, "bufferSize", bufferSize)
}

// NewInvalidFormatError creates a new invalid format error.
func NewInvalidFormatError(format string) *Error {
	err := NewError(ErrInvalidFormat, "Data format is invalid")
	return WithContext(err, "format", format)
}

// NewSchemaNotFoundError creates a new schema not found error.
func NewSchemaNotFoundError(schemaName string) *Error {
	err := NewError(ErrSchemaNotFound, "Schema not found")
	return WithContext(err, "schemaName", schemaName)
}

// IsErrorCode checks if an error has a specific error code.
func IsErrorCode(err error, code ErrorCode) bool {
	if err == nil {
		return false
	}

	var customErr *Error
	if e, ok := err.(*Error); ok {
		customErr = e
	} else {
		// Try to unwrap standard errors to find our custom error
		for e := err; e != nil; {
			if customError, ok := e.(*Error); ok {
				customErr = customError
				break
			}

			// Use type assertion for Go 1.13+ error unwrapping
			u, ok := e.(interface {
				Unwrap() error
			})
			if !ok {
				break
			}
			e = u.Unwrap()
		}
	}

	return customErr != nil && customErr.Code == code
}

// GetErrorCode extracts the error code from an error, returning ErrUnknown if not found.
func GetErrorCode(err error) ErrorCode {
	if err == nil {
		return ErrUnknown
	}

	var customErr *Error
	if e, ok := err.(*Error); ok {
		customErr = e
	} else {
		// Try to unwrap standard errors to find our custom error
		for e := err; e != nil; {
			if customError, ok := e.(*Error); ok {
				customErr = customError
				break
			}

			// Use type assertion for Go 1.13+ error unwrapping
			u, ok := e.(interface {
				Unwrap() error
			})
			if !ok {
				break
			}
			e = u.Unwrap()
		}
	}

	if customErr != nil {
		return customErr.Code
	}

	return ErrUnknown
}

//Personal.AI order the ending
