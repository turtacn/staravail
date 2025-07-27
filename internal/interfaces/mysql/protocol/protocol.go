// Package protocol provides MySQL protocol handling functionality.
package protocol

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/domain/models"
	"github.com/turtacn/staravail/internal/domain/query"
)

// MySQL protocol constants
const (
	// MySQL packet header size
	packetHeaderSize = 4

	// MySQL packet max size
	maxPacketSize = 16 * 1024 * 1024 // 16MB

	// MySQL protocol version
	protocolVersion byte = 10

	// MySQL default max allowed packet size
	defaultMaxAllowedPacket = 4 * 1024 * 1024 // 4MB

	// Character set constants
	charsetUtf8    = 33 // utf8_general_ci
	charsetBinary  = 63 // binary
	charsetUtf8mb4 = 45 // utf8mb4_general_ci
)

// MySQL capability flags
const (
	CapabilityClientLongPassword uint32 = 1 << iota
	CapabilityClientFoundRows
	CapabilityClientLongFlag
	CapabilityClientConnectWithDB
	CapabilityClientNoSchema
	CapabilityClientCompress
	CapabilityClientODBC
	CapabilityClientLocalFiles
	CapabilityClientIgnoreSpace
	CapabilityClientProtocol41
	CapabilityClientInteractive
	CapabilityClientSSL
	CapabilityClientIgnoreSIGPIPE
	CapabilityClientTransactions
	CapabilityClientReserved
	CapabilityClientSecureConnection
	CapabilityClientMultiStatements
	CapabilityClientMultiResults
	CapabilityClientPSMultiResults
	CapabilityClientPluginAuth
	CapabilityClientConnectAttrs
	CapabilityClientPluginAuthLenEncClientData
	CapabilityClientCanHandleExpiredPasswords
	CapabilityClientSessionTrack
	CapabilityClientDeprecateEOF
	CapabilityClientOptionalResultsetMetadata
	CapabilityClientZstdCompressionAlgorithm
	CapabilityClientCapabilityExtension = 1 << 29
	CapabilityClientMultiFactorAuthentication
	CapabilityClientTLSv13
	CapabilityClientCompressionZstdUpstreamCapable
)

// Default server capabilities
const DefaultServerCapabilities = CapabilityClientLongPassword |
	CapabilityClientFoundRows |
	CapabilityClientLongFlag |
	CapabilityClientConnectWithDB |
	CapabilityClientProtocol41 |
	CapabilityClientTransactions |
	CapabilityClientSecureConnection |
	CapabilityClientMultiResults |
	CapabilityClientPluginAuth |
	CapabilityClientPluginAuthLenEncClientData

// MySQL server status flags
const (
	ServerStatusInTrans uint16 = 1 << iota
	ServerStatusAutocommit
	ServerMoreResultsExists
	ServerStatusNoGoodIndexUsed
	ServerStatusNoIndexUsed
	ServerStatusCursorExists
	ServerStatusLastRowSent
	ServerStatusDbDropped
	ServerStatusNoBackslashEscapes
	ServerStatusMetadataChanged
	ServerStatusQueryWasSlow
	ServerStatusPSOutParams
	ServerStatusInTransReadonly
	ServerStatusSessionStateChanged
	ServerStatusNoMoreResultsExists
)

// MySQL command types
type CommandType byte

const (
	ComSleep CommandType = iota
	ComQuit
	ComInitDB
	ComQuery
	ComFieldList
	ComCreateDB
	ComDropDB
	ComRefresh
	ComShutdown
	ComStatistics
	ComProcessInfo
	ComConnect
	ComProcessKill
	ComDebug
	ComPing
	ComTime
	ComDelayedInsert
	ComChangeUser
	ComBinlogDump
	ComTableDump
	ComConnectOut
	ComRegisterSlave
	ComStmtPrepare
	ComStmtExecute
	ComStmtSendLongData
	ComStmtClose
	ComStmtReset
	ComSetOption
	ComStmtFetch
)

// MySQL option types
const (
	OptionMultiStatements = iota
)

// String returns the string representation of the command
func (c CommandType) String() string {
	switch c {
	case ComQuit:
		return "COM_QUIT"
	case ComInitDB:
		return "COM_INIT_DB"
	case ComQuery:
		return "COM_QUERY"
	case ComFieldList:
		return "COM_FIELD_LIST"
	case ComPing:
		return "COM_PING"
	case ComStmtPrepare:
		return "COM_STMT_PREPARE"
	case ComStmtExecute:
		return "COM_STMT_EXECUTE"
	case ComStmtClose:
		return "COM_STMT_CLOSE"
	case ComStmtReset:
		return "COM_STMT_RESET"
	case ComSetOption:
		return "COM_SET_OPTION"
	default:
		return fmt.Sprintf("COM_UNKNOWN(%d)", c)
	}
}

// MySQL field types
const (
	FieldTypeDecimal byte = iota
	FieldTypeTiny
	FieldTypeShort
	FieldTypeLong
	FieldTypeFloat
	FieldTypeDouble
	FieldTypeNull
	FieldTypeTimestamp
	FieldTypeLongLong
	FieldTypeInt24
	FieldTypeDate
	FieldTypeTime
	FieldTypeDateTime
	FieldTypeYear
	FieldTypeNewDate
	FieldTypeVarchar
	FieldTypeBit
)

const (
	FieldTypeJSON byte = iota + 0xf5
	FieldTypeNewDecimal
	FieldTypeEnum
	FieldTypeSet
	FieldTypeTinyBlob
	FieldTypeMediumBlob
	FieldTypeLongBlob
	FieldTypeBlob
	FieldTypeVarString
	FieldTypeString
	FieldTypeGeometry
)

// MySQL field flags
const (
	FieldFlagNotNull uint16 = 1 << iota
	FieldFlagPrimaryKey
	FieldFlagUniqueKey
	FieldFlagMultipleKey
	FieldFlagBlob
	FieldFlagUnsigned
	FieldFlagZeroFill
	FieldFlagBinary
	FieldFlagEnum
	FieldFlagAutoIncrement
	FieldFlagTimestamp
	FieldFlagSet
	FieldFlagUnknown1
	FieldFlagUnknown2
	FieldFlagUnknown3
	FieldFlagUnknown4
)

const (
	FieldFlagNone uint16 = 0
)

// Error codes
const (
	ErrUnknown              uint16 = 1000
	ErrAccessDenied         uint16 = 1045
	ErrNoDb                 uint16 = 1046
	ErrUnknownCommand       uint16 = 1047
	ErrDbAccessDenied       uint16 = 1044
	ErrBadDb                uint16 = 1049
	ErrTableExists          uint16 = 1050
	ErrBadTable             uint16 = 1051
	ErrDupKey               uint16 = 1062
	ErrParse                uint16 = 1064
	ErrNoSuchTable          uint16 = 1146
	ErrCantDoThisDuringTxn  uint16 = 1179
	ErrMalformPacket        uint16 = 1835
	ErrBadField             uint16 = 1054
	ErrQueryTimeout         uint16 = 1969
	ErrRowIsReferenced      uint16 = 1217
	ErrConnectTimeout       uint16 = 2002
	ErrMaxUserConnections   uint16 = 1203
	ErrUnknownStmtHandler   uint16 = 1243
)

// MySQLError represents a MySQL error packet.
type MySQLError struct {
	Code    uint16
	Message string
	State   string
}

// Error returns the error message.
func (e *MySQLError) Error() string {
	return fmt.Sprintf("MySQL Error %d: %s", e.Code, e.Message)
}

// Common errors
var (
	ErrConnectionClosed = errors.New("connection closed")
	ErrMalformedPacket  = errors.New("malformed packet")
	ErrInvalidSequence  = errors.New("invalid sequence")
)

// Field represents a column definition.
type Field struct {
	Database string
	Table    string
	OrgTable string
	Name     string
	OrgName  string
	Charset  uint8
	Length   uint32
	Type     uint8
	Flags    uint16
	Decimals uint8
}

// HandshakePacket represents the initial handshake packet sent by the server.
type HandshakePacket struct {
	ProtocolVersion byte
	ServerVersion   string
	ConnectionID    uint32
	AuthPluginData  []byte
	CapabilityFlags uint32
	CharacterSet    uint8
	StatusFlags     uint16
	AuthPluginName  string
}

// HandshakeResponsePacket represents the handshake response packet sent by the client.
type HandshakeResponsePacket struct {
	CapabilityFlags uint32
	MaxPacketSize   uint32
	CharacterSet    uint8
	Username        string
	AuthResponse    []byte
	Database        string
	AuthPluginName  string
	ConnectAttrs    map[string]string
}

// ProtocolHandler defines the interface for MySQL protocol handling.
type ProtocolHandler interface {
	// Command handling
	HandleCommand(cmd CommandType, data []byte) error
	HandleQuery(query string) error
	HandleStmtPrepare(query string) error
	HandleStmtExecute(stmtID uint32, params []interface{}) error

	// Result writing
	WriteResultSet(columns []*Field, rows [][]interface{}) error
	WriteError(code uint16, message string) error
	WriteOK(affectedRows uint64, lastInsertID uint64, status uint16, warnings uint16) error

	// Authentication
	HandleHandshake() error
	VerifyAuth(username, password string, salt []byte) bool

	// Connection management
	Close() error

	// Session management
	SetDatabase(dbName string) error
	GetDatabase() string
	SetVariable(name, value string) error
	GetVariable(name string) string
}

// MySQLProtocol implements the MySQL protocol handling.
type MySQLProtocol struct {
	conn             net.Conn
	reader           *bytes.Reader
	buffer           []byte
	sequence         uint8
	maxPacketSize    int
	maxAllowedPacket int
	logger           *zap.Logger
}

// NewMySQLProtocol creates a new MySQL protocol handler.
func NewMySQLProtocol(conn net.Conn) *MySQLProtocol {
	return &MySQLProtocol{
		conn:             conn,
		buffer:           make([]byte, 4096),
		maxPacketSize:    maxPacketSize,
		maxAllowedPacket: defaultMaxAllowedPacket,
		logger:           logging.GetLogger().Named("mysql.protocol"),
	}
}

// readPacket reads a full packet from the connection.
func (p *MySQLProtocol) readPacket() ([]byte, error) {
	// Read packet header
	headerBuf := make([]byte, packetHeaderSize)
	if _, err := io.ReadFull(p.conn, headerBuf); err != nil {
		if err == io.EOF {
			return nil, ErrConnectionClosed
		}
		return nil, fmt.Errorf("read packet header: %w", err)
	}

	// Parse packet length and sequence
	length := int(uint32(headerBuf[0]) | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])<<16)
	sequence := headerBuf[3]

	// Validate packet length
	if length == 0 {
		return []byte{}, nil
	}

	if length > p.maxPacketSize {
		return nil, fmt.Errorf("packet too large: %d > %d", length, p.maxPacketSize)
	}

	// Check sequence
	if sequence != p.sequence {
		return nil, fmt.Errorf("invalid sequence: got %d, expected %d", sequence, p.sequence)
	}

	// Increment sequence for next packet
	p.sequence++

	// Read packet data
	data := make([]byte, length)
	if _, err := io.ReadFull(p.conn, data); err != nil {
		return nil, fmt.Errorf("read packet data: %w", err)
	}

	// Handle multi-packet sequences for large packets
	if length == 0xffffff {
		// This is a multi-packet sequence
		nextData, err := p.readPacket()
		if err != nil {
			return nil, err
		}

		// Append the next packet's data
		data = append(data, nextData...)
	}

	return data, nil
}

// writePacket writes a packet to the connection.
func (p *MySQLProtocol) writePacket(data []byte) error {
	// Initialize length and position
	dataLen := len(data)
	pos := 0

	for {
		// Calculate the size of this chunk
		size := dataLen - pos
		if size > 0xffffff {
			size = 0xffffff
		}

		// Prepare header
		header := make([]byte, packetHeaderSize)
		header[0] = byte(size)
		header[1] = byte(size >> 8)
		header[2] = byte(size >> 16)
		header[3] = p.sequence

		// Increment sequence for next packet
		p.sequence++

		// Write header
		if _, err := p.conn.Write(header); err != nil {
			return fmt.Errorf("write packet header: %w", err)
		}

		// Write data chunk
		if _, err := p.conn.Write(data[pos : pos+size]); err != nil {
			return fmt.Errorf("write packet data: %w", err)
		}

		// Move position
		pos += size

		// Check if we're done
		if pos >= dataLen {
			break
		}
	}

	return nil
}

// resetSequence resets the packet sequence number.
func (p *MySQLProtocol) resetSequence() {
	p.sequence = 0
}

// ReadCommandPacket reads a command packet from the client.
func (p *MySQLProtocol) ReadCommandPacket() (CommandType, []byte, error) {
	// Reset sequence for new command
	p.resetSequence()

	// Read packet
	data, err := p.readPacket()
	if err != nil {
		return 0, nil, err
	}

	if len(data) == 0 {
		return 0, nil, ErrMalformedPacket
	}

	// Extract command type and data
	cmd := CommandType(data[0])
	var cmdData []byte
	if len(data) > 1 {
		cmdData = data[1:]
	}

	return cmd, cmdData, nil
}

// WriteHandshake writes the initial handshake packet to the client.
func (p *MySQLProtocol) WriteHandshake(handshake *HandshakePacket) error {
	// Reset sequence for new connection
	p.resetSequence()

	// Prepare handshake packet
	buffer := make([]byte, 0, 128)

	// Protocol version
	buffer = append(buffer, handshake.ProtocolVersion)

	// Server version
	buffer = append(buffer, []byte(handshake.ServerVersion)...)
	buffer = append(buffer, 0) // NULL terminator

	// Connection ID
	buffer = append(buffer, byte(handshake.ConnectionID), byte(handshake.ConnectionID>>8),
		byte(handshake.ConnectionID>>16), byte(handshake.ConnectionID>>24))

	// Auth plugin data part 1 (8 bytes)
	buffer = append(buffer, handshake.AuthPluginData[:8]...)

	// Filler
	buffer = append(buffer, 0)

	// Capability flags (lower 2 bytes)
	buffer = append(buffer, byte(handshake.CapabilityFlags), byte(handshake.CapabilityFlags>>8))

	// Character set
	buffer = append(buffer, handshake.CharacterSet)

	// Status flags
	buffer = append(buffer, byte(handshake.StatusFlags), byte(handshake.StatusFlags>>8))

	// Capability flags (upper 2 bytes)
	buffer = append(buffer, byte(handshake.CapabilityFlags>>16), byte(handshake.CapabilityFlags>>24))

	// Length of auth plugin data
	authDataLen := len(handshake.AuthPluginData)
	if authDataLen < 8 {
		authDataLen = 0
	} else {
		authDataLen = 8 + (authDataLen - 8)
	}
	buffer = append(buffer, byte(authDataLen))

	// Reserved (10 bytes of 0)
	buffer = append(buffer, bytes.Repeat([]byte{0}, 10)...)

	// Auth plugin data part 2
	if len(handshake.AuthPluginData) > 8 {
		buffer = append(buffer, handshake.AuthPluginData[8:]...)
	}
	buffer = append(buffer, 0) // NULL terminator

	// Auth plugin name
	buffer = append(buffer, []byte(handshake.AuthPluginName)...)
	buffer = append(buffer, 0) // NULL terminator

	// Write packet
	return p.writePacket(buffer)
}

// ReadHandshakeResponse reads the handshake response packet from the client.
func (p *MySQLProtocol) ReadHandshakeResponse() (*HandshakeResponsePacket, error) {
	// Read packet
	data, err := p.readPacket()
	if err != nil {
		return nil, err
	}

	if len(data) < 32 {
		return nil, ErrMalformedPacket
	}

	// Create response packet
	resp := &HandshakeResponsePacket{}

	// Parse data
	pos := 0

	// Capability flags (4 bytes)
	resp.CapabilityFlags = uint32(data[pos]) | uint32(data[pos+1])<<8 |
		uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
	pos += 4

	// Max packet size (4 bytes)
	resp.MaxPacketSize = uint32(data[pos]) | uint32(data[pos+1])<<8 |
		uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
	pos += 4

	// Character set (1 byte)
	resp.CharacterSet = data[pos]
	pos += 1

	// Reserved (23 bytes)
	pos += 23

	// Username (null-terminated string)
	nullPos := bytes.IndexByte(data[pos:], 0)
	if nullPos == -1 {
		return nil, ErrMalformedPacket
	}
	resp.Username = string(data[pos : pos+nullPos])
	pos += nullPos + 1

	// Auth response
	if (resp.CapabilityFlags & CapabilityClientPluginAuthLenEncClientData) != 0 {
		// Length-encoded
		if pos >= len(data) {
			return nil, ErrMalformedPacket
		}

		// Read length-encoded binary
		length, lenSize, err := readLengthEncodedInteger(data, pos)
		if err != nil {
			return nil, err
		}
		pos += lenSize

		if pos+int(length) > len(data) {
			return nil, ErrMalformedPacket
		}

		resp.AuthResponse = data[pos : pos+int(length)]
		pos += int(length)
	} else if (resp.CapabilityFlags & CapabilityClientSecureConnection) != 0 {
		// Length prefixed
		if pos >= len(data) {
			return nil, ErrMalformedPacket
		}

		length := int(data[pos])
		pos++

		if pos+length > len(data) {
			return nil, ErrMalformedPacket
		}

		resp.AuthResponse = data[pos : pos+length]
		pos += length
	} else {
		// Null-terminated
		nullPos := bytes.IndexByte(data[pos:], 0)
		if nullPos == -1 {
			return nil, ErrMalformedPacket
		}

		resp.AuthResponse = data[pos : pos+nullPos]
		pos += nullPos + 1
	}

	// Database name (if capabilities & CLIENT_CONNECT_WITH_DB)
	if (resp.CapabilityFlags & CapabilityClientConnectWithDB) != 0 && pos < len(data) {
		nullPos := bytes.IndexByte(data[pos:], 0)
		if nullPos == -1 {
			nullPos = len(data) - pos
		}

		resp.Database = string(data[pos : pos+nullPos])
		pos += nullPos + 1
	}

	// Auth plugin name (if capabilities & CLIENT_PLUGIN_AUTH)
	if (resp.CapabilityFlags & CapabilityClientPluginAuth) != 0 && pos < len(data) {
		nullPos := bytes.IndexByte(data[pos:], 0)
		if nullPos == -1 {
			nullPos = len(data) - pos
		}

		resp.AuthPluginName = string(data[pos : pos+nullPos])
		pos += nullPos + 1
	}

	// Connect attributes (if capabilities & CLIENT_CONNECT_ATTRS)
	if (resp.CapabilityFlags & CapabilityClientConnectAttrs) != 0 && pos < len(data) {
		// Read length-encoded binary
		length, lenSize, err := readLengthEncodedInteger(data, pos)
		if err != nil {
			return nil, err
		}
		pos += lenSize

		if pos+int(length) > len(data) {
			return nil, ErrMalformedPacket
		}

		// Parse attributes
		resp.ConnectAttrs = make(map[string]string)
		attrEnd := pos + int(length)

		for pos < attrEnd {
			// Read key
			keyLen, lenSize, err := readLengthEncodedInteger(data, pos)
			if err != nil {
				return nil, err
			}
			pos += lenSize

			if pos+int(keyLen) > attrEnd {
				return nil, ErrMalformedPacket
			}

			key := string(data[pos : pos+int(keyLen)])
			pos += int(keyLen)

			// Read value
			valueLen, lenSize, err := readLengthEncodedInteger(data, pos)
			if err != nil {
				return nil, err
			}
			pos += lenSize

			if pos+int(valueLen) > attrEnd {
				return nil, ErrMalformedPacket
			}

			value := string(data[pos : pos+int(valueLen)])
			pos += int(valueLen)

			resp.ConnectAttrs[key] = value
		}
	}

	return resp, nil
}

// WriteOK writes an OK packet to the client.
func (p *MySQLProtocol) WriteOK(affectedRows, lastInsertID uint64, status uint16, warnings uint16) error {
	// Prepare OK packet
	buffer := make([]byte, 0, 12)

	// Header (0x00)
	buffer = append(buffer, 0x00)

	// Affected rows (length-encoded integer)
	buffer = writeLengthEncodedInteger(buffer, affectedRows)

	// Last insert ID (length-encoded integer)
	buffer = writeLengthEncodedInteger(buffer, lastInsertID)

	// Status flags
	buffer = append(buffer, byte(status), byte(status>>8))

	// Warnings
	buffer = append(buffer, byte(warnings), byte(warnings>>8))

	// Write packet
	return p.writePacket(buffer)
}

// WriteError writes an error packet to the client.
func (p *MySQLProtocol) WriteError(code uint16, message string) error {
	// Prepare error packet
	buffer := make([]byte, 0, 9+len(message))

	// Header (0xff)
	buffer = append(buffer, 0xff)

	// Error code
	buffer = append(buffer, byte(code), byte(code>>8))

	// SQL state marker (#)
	buffer = append(buffer, '#')

	// SQL state (5 bytes)
	buffer = append(buffer, []byte("HY000")...)

	// Error message
	buffer = append(buffer, []byte(message)...)

	// Write packet
	return p.writePacket(buffer)
}

// WriteEOF writes an EOF packet to the client.
func (p *MySQLProtocol) WriteEOF(status uint16) error {
	// Prepare EOF packet
	buffer := make([]byte, 5)

	// Header (0xfe)
	buffer[0] = 0xfe

	// Warnings (0)
	buffer[1] = 0
	buffer[2] = 0

	// Status flags
	buffer[3] = byte(status)
	buffer[4] = byte(status >> 8)

	// Write packet
	return p.writePacket(buffer)
}

// WriteResultSetHeader writes the header for a result set.
func (p *MySQLProtocol) WriteResultSetHeader(columnCount int) error {
	// Write column count as length-encoded integer
	buffer := make([]byte, 0, 9)
	buffer = writeLengthEncodedInteger(buffer, uint64(columnCount))

	// Write packet
	return p.writePacket(buffer)
}

// WriteColumnDefinition writes a column definition packet.
func (p *MySQLProtocol) WriteColumnDefinition(field *Field) error {
	// Prepare column definition packet
	buffer := make([]byte, 0, 128)

	// Catalog (always "def")
	buffer = writeLengthEncodedString(buffer, "def")

	// Schema (database)
	buffer = writeLengthEncodedString(buffer, field.Database)

	// Table
	buffer = writeLengthEncodedString(buffer, field.Table)

	// Original table
	buffer = writeLengthEncodedString(buffer, field.OrgTable)

	// Column name
	buffer = writeLengthEncodedString(buffer, field.Name)

	// Original column name
	buffer = writeLengthEncodedString(buffer, field.OrgName)

	// Fixed length fields
	buffer = append(buffer, 0x0c) // Length of fixed-length fields

	// Character set
	buffer = append(buffer, field.Charset, 0x00)

	// Column length
	buffer = append(buffer, byte(field.Length), byte(field.Length>>8),
		byte(field.Length>>16), byte(field.Length>>24))

	// Column type
	buffer = append(buffer, field.Type)

	// Column flags
	buffer = append(buffer, byte(field.Flags), byte(field.Flags>>8))

	// Decimals
	buffer = append(buffer, field.Decimals)

	// Filler (2 bytes)
	buffer = append(buffer, 0x00, 0x00)

	// Write packet
	return p.writePacket(buffer)
}

// WriteTextRow writes a text-format row to the client.
func (p *MySQLProtocol) WriteTextRow(row []interface{}) error {
	// Prepare row packet
	buffer := make([]byte, 0, 128)

	// Add each column
	for _, val := range row {
		if val == nil {
			// NULL value
			buffer = append(buffer, 0xfb)
			continue
		}

		// Convert value to string based on type
		var str string
		switch v := val.(type) {
		case string:
			str = v
		case []byte:
			str = string(v)
		case int:
			str = fmt.Sprintf("%d", v)
		case int64:
			str = fmt.Sprintf("%d", v)
		case uint64:
			str = fmt.Sprintf("%d", v)
		case float32:
			str = fmt.Sprintf("%f", v)
		case float64:
			str = fmt.Sprintf("%f", v)
		case bool:
			if v {
				str = "1"
			} else {
				str = "0"
			}
		case time.Time:
			str = v.Format("2006-01-02 15:04:05.999999")
		default:
			str = fmt.Sprintf("%v", v)
		}

		// Write length-encoded string
		buffer = writeLengthEncodedString(buffer, str)
	}

	// Write packet
	return p.writePacket(buffer)
}

// WriteBinaryRow writes a binary-format row to the client.
func (p *MySQLProtocol) WriteBinaryRow(row []interface{}, paramTypes []byte) error {
	// Prepare row packet
	buffer := make([]byte, 0, 128)

	// Row header (0x00)
	buffer = append(buffer, 0x00)

	// NULL bitmap
	nullBitmapSize := (len(row) + 7 + 2) / 8
	nullBitmap := make([]byte, nullBitmapSize)

	// Add each column
	for i, val := range row {
		if val == nil {
			// Set NULL bit
			nullBitmap[(i+2)/8] |= 1 << ((i + 2) % 8)
			continue
		}

		// For non-NULL values, we'll fill in the values later
	}

	// Add NULL bitmap to buffer
	buffer = append(buffer, nullBitmap...)

	// Add values
	for i, val := range row {
		if val == nil {
			// NULL values are handled by the bitmap
			continue
		}

		// Get parameter type
		var paramType byte
		if i < len(paramTypes) {
			paramType = paramTypes[i]
		} else {
			paramType = guessType(val)
		}

		// Encode value based on type
		switch paramType {
		case FieldTypeTiny:
			// TINYINT
			var v byte
			switch val := val.(type) {
			case int:
				v = byte(val)
			case int64:
				v = byte(val)
			case uint64:
				v = byte(val)
			case bool:
				if val {
					v = 1
				}
			default:
				// Try to convert to int
				v = 0
			}
			buffer = append(buffer, v)

		case FieldTypeShort, FieldTypeYear:
			// SMALLINT or YEAR
			var v int16
			switch val := val.(type) {
			case int:
				v = int16(val)
			case int64:
				v = int16(val)
			case uint64:
				v = int16(val)
			default:
				// Try to convert to int
				v = 0
			}
			buffer = append(buffer, byte(v), byte(v>>8))

		case FieldTypeInt24, FieldTypeLong:
			// MEDIUMINT or INT
			var v int32
			switch val := val.(type) {
			case int:
				v = int32(val)
			case int64:
				v = int32(val)
			case uint64:
				v = int32(val)
			default:
				// Try to convert to int
				v = 0
			}
			buffer = append(buffer, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))

		case FieldTypeLongLong:
			// BIGINT
			var v int64
			switch val := val.(type) {
			case int:
				v = int64(val)
			case int64:
				v = val
			case uint64:
				v = int64(val)
			default:
				// Try to convert to int
				v = 0
			}
			buffer = append(buffer, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
				byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))

		case FieldTypeFloat:
			// FLOAT
			var v float32
			switch val := val.(type) {
			case float32:
				v = val
			case float64:
				v = float32(val)
			default:
				// Try to convert to float
				v = 0
			}
			bits := math.Float32bits(v)
			buffer = append(buffer, byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24))

		case FieldTypeDouble:
			// DOUBLE
			var v float64
			switch val := val.(type) {
			case float32:
				v = float64(val)
			case float64:
				v = val
			default:
				// Try to convert to float
				v = 0
			}
			bits := math.Float64bits(v)
			buffer = append(buffer, byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24),
				byte(bits>>32), byte(bits>>40), byte(bits>>48), byte(bits>>56))

		case FieldTypeTimestamp, FieldTypeDateTime:
			// TIMESTAMP or DATETIME
			var t time.Time
			switch val := val.(type) {
			case time.Time:
				t = val
			case string:
				// Try to parse string as time
				t, _ = time.Parse("2006-01-02 15:04:05", val)
			default:
				t = time.Now()
			}

			// Format: length (1) + year (2) + month (1) + day (1) + hour (1) + minute (1) + second (1) + microsecond (4)
			if t.IsZero() {
				// Zero value
				buffer = append(buffer, 0)
			} else {
				usec := t.Nanosecond() / 1000
				if usec == 0 {
					// Without microsecond
					buffer = append(buffer, 7) // Length
					buffer = append(buffer, byte(t.Year()), byte(t.Year()>>8))
					buffer = append(buffer, byte(t.Month()))
					buffer = append(buffer, byte(t.Day()))
					buffer = append(buffer, byte(t.Hour()))
					buffer = append(buffer, byte(t.Minute()))
					buffer = append(buffer, byte(t.Second()))
				} else {
					// With microsecond
					buffer = append(buffer, 11) // Length
					buffer = append(buffer, byte(t.Year()), byte(t.Year()>>8))
					buffer = append(buffer, byte(t.Month()))
					buffer = append(buffer, byte(t.Day()))
					buffer = append(buffer, byte(t.Hour()))
					buffer = append(buffer, byte(t.Minute()))
					buffer = append(buffer, byte(t.Second()))
					buffer = append(buffer, byte(usec), byte(usec>>8), byte(usec>>16), byte(usec>>24))
				}
			}

		case FieldTypeDate:
			// DATE
			var t time.Time
			switch val := val.(type) {
			case time.Time:
				t = val
			case string:
				// Try to parse string as date
				t, _ = time.Parse("2006-01-02", val)
			default:
				t = time.Now()
			}

			if t.IsZero() {
				// Zero value
				buffer = append(buffer, 0)
			} else {
				// Format: length (1) + year (2) + month (1) + day (1)
				buffer = append(buffer, 4) // Length
				buffer = append(buffer, byte(t.Year()), byte(t.Year()>>8))
				buffer = append(buffer, byte(t.Month()))
				buffer = append(buffer, byte(t.Day()))
			}

		case FieldTypeTime:
			// TIME
			var t time.Time
			switch val := val.(type) {
			case time.Time:
				t = val
			case string:
				// Try to parse string as time
				t, _ = time.Parse("15:04:05", val)
			default:
				t = time.Now()
			}

			if t.IsZero() {
				// Zero value
				buffer = append(buffer, 0)
			} else {
				// Format: length (1) + is_negative (1) + hour (4) + minute (1) + second (1) + microsecond (4)
				usec := t.Nanosecond() / 1000
				if usec == 0 {
					// Without microsecond
					buffer = append(buffer, 8) // Length
					buffer = append(buffer, 0) // Not negative
					buffer = append(buffer, byte(t.Hour()), 0, 0, 0)
					buffer = append(buffer, byte(t.Minute()))
					buffer = append(buffer, byte(t.Second()))
				} else {
					// With microsecond
					buffer = append(buffer, 12) // Length
					buffer = append(buffer, 0)  // Not negative
					buffer = append(buffer, byte(t.Hour()), 0, 0, 0)
					buffer = append(buffer, byte(t.Minute()))
					buffer = append(buffer, byte(t.Second()))
					buffer = append(buffer, byte(usec), byte(usec>>8), byte(usec>>16), byte(usec>>24))
				}
			}

		case FieldTypeVarString, FieldTypeString, FieldTypeVarchar, FieldTypeBlob:
			// STRING, VARCHAR, TEXT, BLOB
			var str string
			switch v := val.(type) {
			case string:
				str = v
			case []byte:
				str = string(v)
			default:
				str = fmt.Sprintf("%v", v)
			}

			// Write as length-encoded string
			buffer = writeLengthEncodedString(buffer, str)

		default:
			// Unknown type, write as string
			str := fmt.Sprintf("%v", val)
			buffer = writeLengthEncodedString(buffer, str)
		}
	}

	// Write packet
	return p.writePacket(buffer)
}

// WritePrepareOK writes a COM_STMT_PREPARE OK packet.
func (p *MySQLProtocol) WritePrepareOK(stmtID uint32, numParams, numColumns uint16) error {
	// Prepare packet
	buffer := make([]byte, 12)

	// Header (0x00)
	buffer[0] = 0x00

	// Statement ID
	buffer[1] = byte(stmtID)
	buffer[2] = byte(stmtID >> 8)
	buffer[3] = byte(stmtID >> 16)
	buffer[4] = byte(stmtID >> 24)

	// Column count
	buffer[5] = byte(numColumns)
	buffer[6] = byte(numColumns >> 8)

	// Parameter count
	buffer[7] = byte(numParams)
	buffer[8] = byte(numParams >> 8)

	// Reserved (1 byte)
	buffer[9] = 0x00

	// Warning count
	buffer[10] = 0x00
	buffer[11] = 0x00

	// Write packet
	return p.writePacket(buffer)
}

// ReadExecuteParameters reads parameters from a COM_STMT_EXECUTE packet.
func (p *MySQLProtocol) ReadExecuteParameters(data []byte, paramCount int) ([]string, error) {
	if len(data) < 9 || paramCount <= 0 {
		return []string{}, nil
	}

	// Skip statement ID (4 bytes)
	pos := 4

	// Skip flags (1 byte)
	pos++

	// Skip iteration count (4 bytes)
	pos += 4

	// Calculate NULL bitmap size in bytes
	nullBitmapSize := (paramCount + 7) / 8
	if pos+nullBitmapSize > len(data) {
		return nil, ErrMalformedPacket
	}

	// Extract NULL bitmap
	nullBitmap := data[pos : pos+nullBitmapSize]
	pos += nullBitmapSize

	// Check for new parameters bound flag
	newParamsBound := true
	if pos < len(data) {
		newParamsBound = data[pos] == 1
		pos++
	}

	// If no new parameters, return empty array
	if !newParamsBound {
		return []string{}, nil
	}

	// Check parameter type information
	if pos+paramCount*2 > len(data) {
		return nil, ErrMalformedPacket
	}

	// Extract parameter types
	paramTypes := make([]byte, paramCount)
	for i := 0; i < paramCount; i++ {
		// Only care about type, not flags
		paramTypes[i] = data[pos]
		pos += 2 // Type (1 byte) + flags (1 byte)
	}

	// Extract parameter values
	params := make([]string, paramCount)
	for i := 0; i < paramCount; i++ {
		// Check if parameter is NULL
		isNull := (nullBitmap[i/8] & (1 << (i % 8))) != 0
		if isNull {
			params[i] = "NULL"
			continue
		}

		// Process parameter based on type
		switch paramTypes[i] {
		case FieldTypeTiny:
			// TINYINT (1 byte)
			if pos+1 > len(data) {
				return nil, ErrMalformedPacket
			}
			params[i] = fmt.Sprintf("%d", data[pos])
			pos++

		case FieldTypeShort:
			// SMALLINT (2 bytes)
			if pos+2 > len(data) {
				return nil, ErrMalformedPacket
			}
			val := int16(uint16(data[pos]) | uint16(data[pos+1])<<8)
			params[i] = fmt.Sprintf("%d", val)
			pos += 2

		case FieldTypeLong, FieldTypeInt24:
			// INT (4 bytes)
			if pos+4 > len(data) {
				return nil, ErrMalformedPacket
			}
			val := int32(uint32(data[pos]) | uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24)
			params[i] = fmt.Sprintf("%d", val)
			pos += 4

		case FieldTypeLongLong:
			// BIGINT (8 bytes)
			if pos+8 > len(data) {
				return nil, ErrMalformedPacket
			}
			val := int64(uint64(data[pos]) | uint64(data[pos+1])<<8 |
				uint64(data[pos+2])<<16 | uint64(data[pos+3])<<24 |
				uint64(data[pos+4])<<32 | uint64(data[pos+5])<<40 |
				uint64(data[pos+6])<<48 | uint64(data[pos+7])<<56)
			params[i] = fmt.Sprintf("%d", val)
			pos += 8

		case FieldTypeFloat:
			// FLOAT (4 bytes)
			if pos+4 > len(data) {
				return nil, ErrMalformedPacket
			}
			bits := uint32(data[pos]) | uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
			val := math.Float32frombits(bits)
			params[i] = fmt.Sprintf("%f", val)
			pos += 4

		case FieldTypeDouble:
			// DOUBLE (8 bytes)
			if pos+8 > len(data) {
				return nil, ErrMalformedPacket
			}
			bits := uint64(data[pos]) | uint64(data[pos+1])<<8 |
				uint64(data[pos+2])<<16 | uint64(data[pos+3])<<24 |
				uint64(data[pos+4])<<32 | uint64(data[pos+5])<<40 |
				uint64(data[pos+6])<<48 | uint64(data[pos+7])<<56
			val := math.Float64frombits(bits)
			params[i] = fmt.Sprintf("%f", val)
			pos += 8

		case FieldTypeString, FieldTypeVarString, FieldTypeVarchar, FieldTypeBlob:
			// STRING, VARCHAR (length-encoded string)
			if pos >= len(data) {
				return nil, ErrMalformedPacket
			}

			// Read length-encoded binary
			length, lenSize, err := readLengthEncodedInteger(data, pos)
			if err != nil {
				return nil, err
			}
			pos += lenSize

			if pos+int(length) > len(data) {
				return nil, ErrMalformedPacket
			}

			// Extract string value
			params[i] = string(data[pos : pos+int(length)])
			pos += int(length)

		case FieldTypeDate, FieldTypeDateTime, FieldTypeTimestamp:
			// DATE, DATETIME, TIMESTAMP
			if pos >= len(data) {
				return nil, ErrMalformedPacket
			}

			// Get length
			length := int(data[pos])
			pos++

			if pos+length > len(data) {
				return nil, ErrMalformedPacket
			}

			if length == 0 {
				// Zero value
				params[i] = "0000-00-00"
			} else {
				// Extract date components
				year := uint16(data[pos]) | uint16(data[pos+1])<<8
				month := data[pos+2]
				day := data[pos+3]

				if length >= 4 {
					// Basic date format
					params[i] = fmt.Sprintf("%04d-%02d-%02d", year, month, day)
				}

				if length >= 7 {
					// Add time
					hour := data[pos+4]
					minute := data[pos+5]
					second := data[pos+6]
					params[i] = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
						year, month, day, hour, minute, second)
				}

				if length == 11 {
					// Add microseconds
					usec := uint32(data[pos+7]) | uint32(data[pos+8])<<8 |
						uint32(data[pos+9])<<16 | uint32(data[pos+10])<<24
					params[i] = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
						year, month, day, hour, minute, second, usec)
				}
			}

			pos += length

		case FieldTypeTime:
			// TIME
			if pos >= len(data) {
				return nil, ErrMalformedPacket
			}

			// Get length
			length := int(data[pos])
			pos++

			if pos+length > len(data) {
				return nil, ErrMalformedPacket
			}

			if length == 0 {
				// Zero value
				params[i] = "00:00:00"
			} else {
				// Skip sign
				isNegative := data[pos] != 0
				pos++

				// Extract time components
				days := uint32(data[pos]) | uint32(data[pos+1])<<8 |
					uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
				hour := data[pos+4]
				minute := data[pos+5]
				second := data[pos+6]

				totalHours := days*24 + uint32(hour)
				prefix := ""
				if isNegative {
					prefix = "-"
				}

				params[i] = fmt.Sprintf("%s%d:%02d:%02d", prefix, totalHours, minute, second)

				if length == 12 {
					// Add microseconds
					usec := uint32(data[pos+7]) | uint32(data[pos+8])<<8 |
						uint32(data[pos+9])<<16 | uint32(data[pos+10])<<24
					params[i] = fmt.Sprintf("%s%d:%02d:%02d.%06d",
						prefix, totalHours, minute, second, usec)
				}
			}

			pos += length

		default:
			// Unknown type, handle as string
			if pos >= len(data) {
				return nil, ErrMalformedPacket
			}

			// Try to read as length-encoded string
			length, lenSize, err := readLengthEncodedInteger(data, pos)
			if err != nil {
				return nil, err
			}
			pos += lenSize

			if pos+int(length) > len(data) {
				return nil, ErrMalformedPacket
			}

			params[i] = string(data[pos : pos+int(length)])
			pos += int(length)
		}
	}

	return params, nil
}

// ReadUint32 reads a 4-byte little-endian uint32 from a byte slice.
func ReadUint32(data []byte, pos int) uint32 {
	return uint32(data[pos]) | uint32(data[pos+1])<<8 |
		uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
}

// ReadUint16 reads a 2-byte little-endian uint16 from a byte slice.
func ReadUint16(data []byte, pos int) uint16 {
	return uint16(data[pos]) | uint16(data[pos+1])<<8
}

// Utility functions

// readLengthEncodedInteger reads a length-encoded integer from a byte slice.
func readLengthEncodedInteger(data []byte, pos int) (uint64, int, error) {
	if pos >= len(data) {
		return 0, 0, ErrMalformedPacket
	}

	switch data[pos] {
	case 0xfb:
		// NULL value
		return 0, 1, nil
	case 0xfc:
		// 2-byte integer
		if pos+3 > len(data) {
			return 0, 0, ErrMalformedPacket
		}
		return uint64(data[pos+1]) | uint64(data[pos+2])<<8, 3, nil
	case 0xfd:
		// 3-byte integer
		if pos+4 > len(data) {
			return 0, 0, ErrMalformedPacket
		}
		return uint64(data[pos+1]) | uint64(data[pos+2])<<8 | uint64(data[pos+3])<<16, 4, nil
	case 0xfe:
		// 8-byte integer
		if pos+9 > len(data) {
			return 0, 0, ErrMalformedPacket
		}
		return uint64(data[pos+1]) | uint64(data[pos+2])<<8 | uint64(data[pos+3])<<16 |
			uint64(data[pos+4])<<24 | uint64(data[pos+5])<<32 | uint64(data[pos+6])<<40 |
			uint64(data[pos+7])<<48 | uint64(data[pos+8])<<56, 9, nil
	default:
		// 1-byte integer
		return uint64(data[pos]), 1, nil
	}
}

// writeLengthEncodedInteger writes a length-encoded integer to a byte slice.
func writeLengthEncodedInteger(buffer []byte, value uint64) []byte {
	if value < 251 {
		// 1-byte integer
		return append(buffer, byte(value))
	} else if value < 65536 {
		// 2-byte integer
		return append(buffer, 0xfc, byte(value), byte(value>>8))
	} else if value < 16777216 {
		// 3-byte integer
		return append(buffer, 0xfd, byte(value), byte(value>>8), byte(value>>16))
	} else {
		// 8-byte integer
		return append(buffer, 0xfe, byte(value), byte(value>>8), byte(value>>16), byte(value>>24),
			byte(value>>32), byte(value>>40), byte(value>>48), byte(value>>56))
	}
}

// writeLengthEncodedString writes a length-encoded string to a byte slice.
func writeLengthEncodedString(buffer []byte, str string) []byte {
	buffer = writeLengthEncodedInteger(buffer, uint64(len(str)))
	return append(buffer, str...)
}

// guessType tries to guess the MySQL field type from a Go value.
func guessType(val interface{}) byte {
	switch val.(type) {
	case bool, int8, uint8:
		return FieldTypeTiny
	case int16, uint16:
		return FieldTypeShort
	case int, int32, uint32:
		return FieldTypeLong
	case int64, uint64:
		return FieldTypeLongLong
	case float32:
		return FieldTypeFloat
	case float64:
		return FieldTypeDouble
	case time.Time:
		return FieldTypeDateTime
	case []byte:
		return FieldTypeBlob
	default:
		return FieldTypeVarString
	}
}

// GenerateRandomBytes generates random bytes for auth.
func GenerateRandomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// If random fails, use time-based fallback
		for i := range b {
			b[i] = byte(time.Now().UnixNano() % 256)
			time.Sleep(1 * time.Nanosecond)
		}
	}
	return b
}

// CalcPassword calculates the MySQL auth hash from password and salt.
func CalcPassword(password string, salt []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// SHA1(password)
	crypt1 := sha1.New()
	crypt1.Write([]byte(password))
	stage1 := crypt1.Sum(nil)

	// SHA1(SHA1(password))
	crypt2 := sha1.New()
	crypt2.Write(stage1)
	stage2 := crypt2.Sum(nil)

	// SHA1(salt + SHA1(SHA1(password)))
	crypt3 := sha1.New()
	crypt3.Write(salt)
	crypt3.Write(stage2)
	stage3 := crypt3.Sum(nil)

	// XOR each byte
	result := make([]byte, len(stage3))
	for i := range stage3 {
		result[i] = stage3[i] ^ stage1[i]
	}

	return result
}

// VerifyAuthResponse verifies a client auth response.
func VerifyAuthResponse(authPluginName string, password string, authResponse []byte, salt []byte) bool {
	switch authPluginName {
	case "mysql_native_password":
		expected := CalcPassword(password, salt)
		if len(expected) == 0 && len(authResponse) == 0 {
			return true
		}
		if len(expected) != len(authResponse) {
			return false
		}
		// Compare each byte
		for i := range expected {
			if expected[i] != authResponse[i] {
				return false
			}
		}
		return true
	default:
		// Unsupported auth method
		return false
	}
}

// ResultsetHandler implements the interface for handling MySQL resultsets.
type ResultsetHandler struct {
	protocol *MySQLProtocol
	logger   *zap.Logger
}

// NewResultsetHandler creates a new resultset handler.
func NewResultsetHandler(protocol *MySQLProtocol) *ResultsetHandler {
	return &ResultsetHandler{
		protocol: protocol,
		logger:   logging.GetLogger().Named("mysql.resultset"),
	}
}

// WriteQueryResult writes a query result to the client.
func (h *ResultsetHandler) WriteQueryResult(result *query.QueryResult) error {
	if result == nil {
		return h.protocol.WriteOK(0, 0, 0, 0)
	}

	// If there are no columns, write an empty result
	if len(result.Columns) == 0 {
		return h.protocol.WriteOK(0, 0, 0, 0)
	}

	// Convert columns to fields
	fields := make([]*Field, len(result.Columns))
	for i, col := range result.Columns {
		fields[i] = &Field{
			Database: col.Database,
			Table:    col.Table,
			OrgTable: col.OrgTable,
			Name:     col.Name,
			OrgName:  col.OrgName,
			Charset:  charsetUtf8,
			Length:   uint32(col.Length),
			Type:     convertToMySQLType(col.Type),
			Flags:    convertToMySQLFlags(col),
			Decimals: uint8(col.Decimals),
		}
	}

	// Write resultset
	if err := h.protocol.WriteResultSetHeader(len(fields)); err != nil {
		return err
	}

	// Write column definitions
	for _, field := range fields {
		if err := h.protocol.WriteColumnDefinition(field); err != nil {
			return err
		}
	}

	// Write EOF after columns
	if err := h.protocol.WriteEOF(0); err != nil {
		return err
	}

	// Write rows
	for _, row := range result.Rows {
		if err := h.protocol.WriteTextRow(row); err != nil {
			return err
		}
	}

	// Write EOF after rows
	return h.protocol.WriteEOF(0)
}

// WritePreparedResult writes a prepared statement result to the client.
func (h *ResultsetHandler) WritePreparedResult(result *query.QueryResult, stmtID uint32, paramTypes []byte) error {
	if result == nil {
		return h.protocol.WriteOK(0, 0, 0, 0)
	}

	// If there are no columns, write an empty result
	if len(result.Columns) == 0 {
		return h.protocol.WriteOK(0, 0, 0, 0)
	}

	// Convert columns to fields
	fields := make([]*Field, len(result.Columns))
	for i, col := range result.Columns {
		fields[i] = &Field{
			Database: col.Database,
			Table:    col.Table,
			OrgTable: col.OrgTable,
			Name:     col.Name,
			OrgName:  col.OrgName,
			Charset:  charsetUtf8,
			Length:   uint32(col.Length),
			Type:     convertToMySQLType(col.Type),
			Flags:    convertToMySQLFlags(col),
			Decimals: uint8(col.Decimals),
		}
	}

	// Write resultset
	if err := h.protocol.WriteResultSetHeader(len(fields)); err != nil {
		return err
	}

	// Write column definitions
	for _, field := range fields {
		if err := h.protocol.WriteColumnDefinition(field); err != nil {
			return err
		}
	}

	// Write EOF after columns
	if err := h.protocol.WriteEOF(0); err != nil {
		return err
	}

	// Write rows in binary format
	for _, row := range result.Rows {
		if err := h.protocol.WriteBinaryRow(row, paramTypes); err != nil {
			return err
		}
	}

	// Write EOF after rows
	return h.protocol.WriteEOF(0)
}

// WriteError writes an error to the client.
func (h *ResultsetHandler) WriteError(err error) error {
	var code uint16 = ErrUnknown
	var message string

	// Convert known errors
	switch {
	case strings.Contains(err.Error(), "syntax error"):
		code = ErrParse
		message = fmt.Sprintf("You have an error in your SQL syntax: %v", err)
	case strings.Contains(err.Error(), "database not found"):
		code = ErrBadDb
		message = err.Error()
	case strings.Contains(err.Error(), "table not found"):
		code = ErrNoSuchTable
		message = err.Error()
	case strings.Contains(err.Error(), "column not found"):
		code = ErrBadField
		message = err.Error()
	case strings.Contains(err.Error(), "permission denied"):
		code = ErrAccessDenied
		message = err.Error()
	case strings.Contains(err.Error(), "timeout"):
		code = ErrQueryTimeout
		message = fmt.Sprintf("Query execution was interrupted, timeout: %v", err)
	default:
		message = err.Error()
	}

	return h.protocol.WriteError(code, message)
}

// Helper functions for type conversion

// convertToMySQLType converts a StarRocks type to a MySQL type.
func convertToMySQLType(typeName string) byte {
	typeName = strings.ToUpper(typeName)

	switch {
	case strings.Contains(typeName, "TINYINT"), strings.Contains(typeName, "BOOLEAN"):
		return FieldTypeTiny
	case strings.Contains(typeName, "SMALLINT"):
		return FieldTypeShort
	case strings.Contains(typeName, "INT"), strings.Contains(typeName, "INTEGER"):
		return FieldTypeLong
	case strings.Contains(typeName, "BIGINT"):
		return FieldTypeLongLong
	case strings.Contains(typeName, "FLOAT"):
		return FieldTypeFloat
	case strings.Contains(typeName, "DOUBLE"):
		return FieldTypeDouble
	case strings.Contains(typeName, "DECIMAL"):
		return FieldTypeNewDecimal
	case strings.Contains(typeName, "DATE"):
		return FieldTypeDate
	case strings.Contains(typeName, "DATETIME"):
		return FieldTypeDateTime
	case strings.Contains(typeName, "TIMESTAMP"):
		return FieldTypeTimestamp
	case strings.Contains(typeName, "TIME"):
		return FieldTypeTime
	case strings.Contains(typeName, "YEAR"):
		return FieldTypeYear
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "VARCHAR"):
		return FieldTypeVarString
	case strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "BLOB"):
		return FieldTypeBlob
	case strings.Contains(typeName, "JSON"):
		return FieldTypeJSON
	case strings.Contains(typeName, "ENUM"):
		return FieldTypeEnum
	case strings.Contains(typeName, "SET"):
		return FieldTypeSet
	default:
		return FieldTypeVarString
	}
}

// convertToMySQLFlags converts StarRocks column attributes to MySQL flags.
func convertToMySQLFlags(col *query.Column) uint16 {
	var flags uint16

	if col.NotNull {
		flags |= FieldFlagNotNull
	}

	if col.PrimaryKey {
		flags |= FieldFlagPrimaryKey
	}

	if col.UniqueKey {
		flags |= FieldFlagUniqueKey
	}

	if col.MultipleKey {
		flags |= FieldFlagMultipleKey
	}

	if col.Unsigned {
		flags |= FieldFlagUnsigned
	}

	if col.Zerofill {
		flags |= FieldFlagZeroFill
	}

	if col.Binary {
		flags |= FieldFlagBinary
	}

	if col.AutoIncrement {
		flags |= FieldFlagAutoIncrement
	}

	if col.Enum {
		flags |= FieldFlagEnum
	}

	if col.Set {
		flags |= FieldFlagSet
	}

	if col.Blob {
		flags |= FieldFlagBlob
	}

	return flags
}

// CommandHandler implements the interface for handling MySQL commands.
type CommandHandler struct {
	protocol       *MySQLProtocol
	resultHandler  *ResultsetHandler
	queryService   interface{} // Will be interfaces.QueryService in real implementation
	writeService   interface{} // Will be interfaces.WriteService in real implementation
	authService    interface{} // Will be interfaces.AuthService in real implementation
	serverStatus   uint16
	currentDB      string
	sessionVars    map[string]string
	preparedStmts  map[uint32]*PreparedStatement
	nextStmtID     uint32
	logger         *zap.Logger
}

// PreparedStatement represents a prepared statement.
type PreparedStatement struct {
	ID           uint32
	Query        string
	ParamCount   uint16
	ColumnCount  uint16
	ParamTypes   []byte
	ColumnFields []*Field
}

// NewCommandHandler creates a new command handler.
func NewCommandHandler(protocol *MySQLProtocol) *CommandHandler {
	return &CommandHandler{
		protocol:      protocol,
		resultHandler: NewResultsetHandler(protocol),
		serverStatus:  ServerStatusAutocommit,
		sessionVars:   make(map[string]string),
		preparedStmts: make(map[uint32]*PreparedStatement),
		logger:        logging.GetLogger().Named("mysql.command"),
	}
}

// SetServices sets the service dependencies.
func (h *CommandHandler) SetServices(queryService, writeService, authService interface{}) {
	h.queryService = queryService
	h.writeService = writeService
	h.authService = authService
}

// HandleCommand handles a MySQL command.
func (h *CommandHandler) HandleCommand(cmd CommandType, data []byte) error {
	switch cmd {
	case ComQuit:
		return ErrConnectionClosed

	case ComInitDB:
		return h.handleInitDB(string(data))

	case ComQuery:
		return h.handleQuery(string(data))

	case ComPing:
		return h.protocol.WriteOK(0, 0, h.serverStatus, 0)

	case ComStmtPrepare:
		return h.handleStmtPrepare(string(data))

	case ComStmtExecute:
		return h.handleStmtExecute(data)

	case ComStmtClose:
		return h.handleStmtClose(data)

	case ComStmtReset:
		return h.handleStmtReset(data)

	case ComFieldList:
		return h.handleFieldList(data)

	case ComSetOption:
		return h.handleSetOption(data)

	default:
		h.logger.Warn("Unsupported command", zap.Uint8("cmd", uint8(cmd)))
		return h.protocol.WriteError(ErrUnknownCommand, fmt.Sprintf("Unsupported command %d", cmd))
	}
}

// handleInitDB handles COM_INIT_DB command.
func (h *CommandHandler) handleInitDB(dbName string) error {
	// In a real implementation, verify that the database exists and the user has access
	h.currentDB = dbName
	return h.protocol.WriteOK(0, 0, h.serverStatus, 0)
}

// handleQuery handles COM_QUERY command.
func (h *CommandHandler) handleQuery(query string) error {
	// Log query
	h.logger.Debug("Query", zap.String("sql", query))

	// Handle special commands
	if h.handleSpecialCommand(query) {
		return nil
	}

	// In a real implementation, parse and execute the query through the query service
	// This is a placeholder to demonstrate the protocol
	result := &query.QueryResult{
		Columns: []*query.Column{
			{Name: "id", Type: "BIGINT", NotNull: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "value", Type: "DOUBLE"},
		},
		Rows: [][]interface{}{
			{int64(1), "Row 1", 10.5},
			{int64(2), "Row 2", 20.75},
			{int64(3), "Row 3", 30.25},
		},
	}

	// Write result to client
	return h.resultHandler.WriteQueryResult(result)
}

// handleSpecialCommand handles special SQL commands.
func (h *CommandHandler) handleSpecialCommand(query string) bool {
	query = strings.TrimSpace(query)
	queryUpper := strings.ToUpper(query)

	switch {
	case strings.HasPrefix(queryUpper, "SET NAMES"):
		// Handle character set
		parts := strings.Fields(query)
		if len(parts) >= 3 {
			charset := strings.Trim(parts[2], "'\"")
			h.sessionVars["character_set_client"] = charset
			h.sessionVars["character_set_connection"] = charset
			h.sessionVars["character_set_results"] = charset
			h.protocol.WriteOK(0, 0, h.serverStatus, 0)
			return true
		}

	case strings.HasPrefix(queryUpper, "SET "):
		// Handle SET commands
		return h.handleSetCommand(query)

	case queryUpper == "BEGIN" || queryUpper == "START TRANSACTION":
		// Begin transaction
		h.serverStatus |= ServerStatusInTrans
		h.protocol.WriteOK(0, 0, h.serverStatus, 0)
		return true

	case queryUpper == "COMMIT":
		// Commit transaction
		h.serverStatus &= ^ServerStatusInTrans
		h.protocol.WriteOK(0, 0, h.serverStatus, 0)
		return true

	case queryUpper == "ROLLBACK":
		// Rollback transaction
		h.serverStatus &= ^ServerStatusInTrans
		h.protocol.WriteOK(0, 0, h.serverStatus, 0)
		return true

	case strings.HasPrefix(queryUpper, "SHOW VARIABLES"):
		// Handle SHOW VARIABLES
		return h.handleShowVariables(query)

	case strings.HasPrefix(queryUpper, "SHOW DATABASES"):
		// Handle SHOW DATABASES
		return h.handleShowDatabases()

	case strings.HasPrefix(queryUpper, "SHOW TABLES"):
		// Handle SHOW TABLES
		return h.handleShowTables()

	case strings.HasPrefix(queryUpper, "USE "):
		// Handle USE database
		parts := strings.Fields(query)
		if len(parts) >= 2 {
			dbName := strings.Trim(parts[1], "`'\"")
			h.currentDB = dbName
			h.protocol.WriteOK(0, 0, h.serverStatus, 0)
			return true
		}
	}

	return false
}

// handleSetCommand handles SET commands.
func (h *CommandHandler) handleSetCommand(query string) bool {
	// Remove SET and split by comma for multiple settings
	settingsStr := strings.TrimPrefix(query, "SET ")
	settings := strings.Split(settingsStr, ",")

	for _, setting := range settings {
		setting = strings.TrimSpace(setting)
		parts := strings.SplitN(setting, "=", 2)
		if len(parts) != 2 {
			continue
		}

		varName := strings.TrimSpace(parts[0])
		varValue := strings.TrimSpace(parts[1])

		// Remove optional @ and quotes
		varName = strings.Trim(varName, "@`'\"")
		varValue = strings.Trim(varValue, "'\"")

		// Handle special variables
		if strings.EqualFold(varName, "autocommit") {
			// Handle autocommit setting
			if strings.EqualFold(varValue, "1") || strings.EqualFold(varValue, "ON") || strings.EqualFold(varValue, "TRUE") {
				h.serverStatus |= ServerStatusAutocommit
			} else {
				h.serverStatus &= ^ServerStatusAutocommit
			}
		}

		// Store in session variables
		h.sessionVars[strings.ToLower(varName)] = varValue
	}

	h.protocol.WriteOK(0, 0, h.serverStatus, 0)
	return true
}

// handleShowVariables handles SHOW VARIABLES command.
func (h *CommandHandler) handleShowVariables(query string) bool {
	// Create a result with Variable_name and Value columns
	result := &query.QueryResult{
		Columns: []*query.Column{
			{Name: "Variable_name", Type: "VARCHAR"},
			{Name: "Value", Type: "VARCHAR"},
		},
		Rows: [][]interface{}{},
	}

	// Add standard variables
	vars := map[string]string{
		"version":                  "5.7.30-StarRocksProxy",
		"version_comment":          "StarRocks Proxy",
		"character_set_client":     h.sessionVars["character_set_client"],
		"character_set_connection": h.sessionVars["character_set_connection"],
		"character_set_results":    h.sessionVars["character_set_results"],
		"character_set_server":     "utf8",
		"autocommit":               (h.serverStatus & ServerStatusAutocommit) != 0 ? "ON" : "OFF",
		"sql_mode":                 "",
		"max_allowed_packet":       "16777216",
		"interactive_timeout":      "28800",
		"wait_timeout":             "28800",
	}

	// Add session variables
	for k, v := range h.sessionVars {
		vars[k] = v
	}

	// Check for LIKE or WHERE clause
	filter := ""
	queryUpper := strings.ToUpper(query)
	if strings.Contains(queryUpper, " LIKE ") {
		parts := strings.Split(queryUpper, " LIKE ")
		if len(parts) >= 2 {
			filter = strings.Trim(parts[1], "'\" ")
			filter = strings.Replace(filter, "%", "", -1)
		}
	}

	// Build result rows
	for k, v := range vars {
		if filter == "" || strings.Contains(k, filter) {
			result.Rows = append(result.Rows, []interface{}{k, v})
		}
	}

	// Write result to client
	h.resultHandler.WriteQueryResult(result)
	return true
}

// handleShowDatabases handles SHOW DATABASES command.
func (h *CommandHandler) handleShowDatabases() bool {
	// In a real implementation, this would query the actual databases
	result := &query.QueryResult{
		Columns: []*query.Column{
			{Name: "Database", Type: "VARCHAR"},
		},
		Rows: [][]interface{}{
			{"information_schema"},
			{"mysql"},
			{"test"},
			{"starrocks"},
		},
	}

	// Write result to client
	h.resultHandler.WriteQueryResult(result)
	return true
}

// handleShowTables handles SHOW TABLES command.
func (h *CommandHandler) handleShowTables() bool {
	if h.currentDB == "" {
		h.protocol.WriteError(ErrNoDb, "No database selected")
		return true
	}

	// In a real implementation, this would query the actual tables
	result := &query.QueryResult{
		Columns: []*query.Column{
			{Name: "Tables_in_" + h.currentDB, Type: "VARCHAR"},
		},
		Rows: [][]interface{}{
			{"users"},
			{"orders"},
			{"products"},
			{"transactions"},
		},
	}

	// Write result to client
	h.resultHandler.WriteQueryResult(result)
	return true
}

// handleStmtPrepare handles COM_STMT_PREPARE command.
func (h *CommandHandler) handleStmtPrepare(query string) error {
	// In a real implementation, parse the query to determine parameters and columns
	// This is a placeholder implementation

	// Create a prepared statement ID
	stmtID := h.nextStmtID
	h.nextStmtID++

	// For demonstration, we'll assume 2 parameters and 3 columns
	paramCount := uint16(2)
	columnCount := uint16(3)

	// Create prepared statement object
	stmt := &PreparedStatement{
		ID:           stmtID,
		Query:        query,
		ParamCount:   paramCount,
		ColumnCount:  columnCount,
		ParamTypes:   []byte{FieldTypeVarString, FieldTypeLong}, // Param types (string, int)
		ColumnFields: make([]*Field, columnCount),
	}

	// Set column fields (would be derived from query in real implementation)
	stmt.ColumnFields[0] = &Field{Name: "id", Type: FieldTypeLongLong, Flags: FieldFlagNotNull}
	stmt.ColumnFields[1] = &Field{Name: "name", Type: FieldTypeVarString}
	stmt.ColumnFields[2] = &Field{Name: "value", Type: FieldTypeDouble}

	// Store prepared statement
	h.preparedStmts[stmtID] = stmt

	// Send prepare response
	if err := h.protocol.WritePrepareOK(stmtID, paramCount, columnCount); err != nil {
		return err
	}

	// Send parameter descriptions if any
	if paramCount > 0 {
		for i := uint16(0); i < paramCount; i++ {
			field := &Field{
				Name:   fmt.Sprintf("param%d", i+1),
				Type:   stmt.ParamTypes[i],
				Flags:  FieldFlagNone,
				Length: 0,
			}
			if err := h.protocol.WriteColumnDefinition(field); err != nil {
				return err
			}
		}

		// Send EOF after parameters
		if err := h.protocol.WriteEOF(h.serverStatus); err != nil {
			return err
		}
	}

	// Send column descriptions if any
	if columnCount > 0 {
		for i := uint16(0); i < columnCount; i++ {
			if err := h.protocol.WriteColumnDefinition(stmt.ColumnFields[i]); err != nil {
				return err
			}
		}

		// Send EOF after columns
		if err := h.protocol.WriteEOF(h.serverStatus); err != nil {
			return err
		}
	}

	return nil
}

// handleStmtExecute handles COM_STMT_EXECUTE command.
func (h *CommandHandler) handleStmtExecute(data []byte) error {
	if len(data) < 4 {
		return h.protocol.WriteError(ErrMalformPacket, "Malformed packet")
	}

	// Get statement ID
	stmtID := ReadUint32(data, 0)

	// Find prepared statement
	stmt, ok := h.preparedStmts[stmtID]
	if !ok {
		return h.protocol.WriteError(ErrUnknownStmtHandler, fmt.Sprintf("Unknown prepared statement ID %d", stmtID))
	}

	// In a real implementation, extract parameters and execute the statement
	// For demonstration, we'll return a fixed result

	// Create sample result
	result := &query.QueryResult{
		Columns: []*query.Column{
			{Name: "id", Type: "BIGINT", NotNull: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "value", Type: "DOUBLE"},
		},
		Rows: [][]interface{}{
			{int64(10), "Prepared Row 1", 100.5},
			{int64(20), "Prepared Row 2", 200.75},
		},
	}

	// Write binary result to client
	return h.resultHandler.WritePreparedResult(result, stmtID, stmt.ParamTypes)
}

// handleStmtClose handles COM_STMT_CLOSE command.
func (h *CommandHandler) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil // Silently ignore invalid packets
	}

	// Get statement ID
	stmtID := ReadUint32(data, 0)

	// Remove prepared statement
	delete(h.preparedStmts, stmtID)

	// No response needed for COM_STMT_CLOSE
	return nil
}

// handleStmtReset handles COM_STMT_RESET command.
func (h *CommandHandler) handleStmtReset(data []byte) error {
	if len(data) < 4 {
		return h.protocol.WriteError(ErrMalformPacket, "Malformed packet")
	}

	// Get statement ID
	stmtID := ReadUint32(data, 0)

	// Find prepared statement
	_, ok := h.preparedStmts[stmtID]
	if !ok {
		return h.protocol.WriteError(ErrUnknownStmtHandler, fmt.Sprintf("Unknown prepared statement ID %d", stmtID))
	}

	// In a real implementation, reset the statement's parameter bindings

	// Send OK packet
	return h.protocol.WriteOK(0, 0, h.serverStatus, 0)
}

// handleFieldList handles COM_FIELD_LIST command.
func (h *CommandHandler) handleFieldList(data []byte) error {
	// Extract table name from data (null-terminated string)
	nullPos := bytes.IndexByte(data, 0)
	if nullPos == -1 {
		return h.protocol.WriteError(ErrMalformPacket, "Malformed COM_FIELD_LIST packet")
	}

	tableName := string(data[:nullPos])

	// For demonstration, we'll return fixed columns for any table
	// In a real implementation, query the actual table structure

	// Create sample fields
	fields := []*Field{
		{Database: h.currentDB, Table: tableName, Name: "id", Type: FieldTypeLongLong, Flags: FieldFlagNotNull | FieldFlagPrimaryKey},
		{Database: h.currentDB, Table: tableName, Name: "name", Type: FieldTypeVarString, Length: 255},
		{Database: h.currentDB, Table: tableName, Name: "created_at", Type: FieldTypeTimestamp},
	}

	// Write column definitions
	for _, field := range fields {
		if err := h.protocol.WriteColumnDefinition(field); err != nil {
			return err
		}
	}

	// Write EOF
	return h.protocol.WriteEOF(h.serverStatus)
}

// handleSetOption handles COM_SET_OPTION command.
func (h *CommandHandler) handleSetOption(data []byte) error {
	if len(data) < 2 {
		return h.protocol.WriteError(ErrMalformPacket, "Malformed COM_SET_OPTION packet")
	}

	option := ReadUint16(data, 0)

	switch option {
	case OptionMultiStatements:
		// Enable or disable multi-statements
		// 0 = disable, 1 = enable
		// In a real implementation, update the connection state

	default:
		return h.protocol.WriteError(ErrUnknown, fmt.Sprintf("Unknown option: %d", option))
	}

	// Send OK packet
	return h.protocol.WriteOK(0, 0, h.serverStatus, 0)
}

// AuthHandler implements authentication handling.
type AuthHandler struct {
	protocol    *MySQLProtocol
	authService interface{} // Will be interfaces.AuthService in real implementation
	logger      *zap.Logger
}

// NewAuthHandler creates a new authentication handler.
func NewAuthHandler(protocol *MySQLProtocol) *AuthHandler {
	return &AuthHandler{
		protocol: protocol,
		logger:   logging.GetLogger().Named("mysql.auth"),
	}
}

// SetAuthService sets the authentication service.
func (h *AuthHandler) SetAuthService(authService interface{}) {
	h.authService = authService
}

// SendHandshake sends the initial handshake packet.
func (h *AuthHandler) SendHandshake(connectionID uint32, serverVersion string) ([]byte, error) {
	// Generate random salt for authentication
	authPluginData := GenerateRandomBytes(20)

	// Create handshake packet
	handshake := &HandshakePacket{
		ProtocolVersion:    protocolVersion,
		ServerVersion:      serverVersion,
		ConnectionID:       connectionID,
		AuthPluginData:     authPluginData,
		CapabilityFlags:    DefaultServerCapabilities,
		CharacterSet:       charsetUtf8,
		StatusFlags:        ServerStatusAutocommit,
		AuthPluginName:     "mysql_native_password",
	}

	// Send handshake packet
	if err := h.protocol.WriteHandshake(handshake); err != nil {
		return nil, err
	}

	return authPluginData, nil
}

// VerifyHandshakeResponse verifies the client's handshake response.
func (h *AuthHandler) VerifyHandshakeResponse(authPluginData []byte) (string, string, bool, error) {
	// Read handshake response from client
	authResp, err := h.protocol.ReadHandshakeResponse()
	if err != nil {
		return "", "", false, err
	}

	// Extract authentication data
	username := authResp.Username
	database := authResp.Database

	// In a real implementation, verify username and password with auth service
	// This is a placeholder for demonstration

	// For demo purposes, accept any user with empty password
	if len(authResp.AuthResponse) == 0 {
		return username, database, true, nil
	}

	// Verify password hash (in a real implementation, check against stored password)
	authSuccess := VerifyAuthResponse("mysql_native_password", "password", authResp.AuthResponse, authPluginData)

	return username, database, authSuccess, nil
}

// SendAuthResult sends the authentication result to the client.
func (h *AuthHandler) SendAuthResult(success bool, message string, serverStatus uint16) error {
	if success {
		return h.protocol.WriteOK(0, 0, serverStatus, 0)
	} else {
		return h.protocol.WriteError(ErrAccessDenied, message)
	}
}
//Personal.AI order the ending
