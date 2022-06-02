
// LabVIEW TypeDescriptors
enum LVTD
{
 	Void = 0x00, // Void type, stores no data
	I8 = 0x01, // Integer with signed 1 byte (8 bit) data
	I16 = 0x02, // Integer with signed 2 byte (16 bit) data
	I32 = 0x03, // Integer with signed 4 byte (32 bit) data
	I64 = 0x04, // Integer with signed 8 byte (64 bit) data
	U8 = 0x05, // Integer with unsigned 1 byte (8 bit) data
	U16 = 0x06, // Integer with unsigned 2 byte (16 bit) data
	U32 = 0x07, // Integer with unsigned 4 byte (32 bit) data
	U64 = 0x08, // Integer with unsigned 8 byte (64 bit) data
	SGL = 0x09, // Floating point with single precision 4 byte data
	DBL = 0x0A, // Floating point with double precision 8 byte data
	EXT = 0x0B, // Floating point with extended (quad precision) data
	CSG = 0x0C, // Complex floating point with 8 byte data
	CDB = 0x0D, // Complex floating point with 16 byte data
	CXT = 0x0E, // Complex floating point with extended (quad precision) data
	Enum_U8 = 0x15, // Enumerated UInt8
	Enum_U16 = 0x16, // Enumerated UInt16
	Enum_U32 = 0x17, // Enumerated UInt32
	Unit_Float32 = 0x19, // Unit_Float32
	Unit_Float64 = 0x1A, // Unit_Float64
	Unit_FloatExt = 0x1B, // Unit_FloatExt
	Unit_Complex64 = 0x1C, // Unit_Complex64
	Unit_Complex128 = 0x1D, // Unit_Complex128
	Unit_ComplexExt = 0x1E, // Unit_ComplexExt
	// 0x20, // Boolean value stored on 2 bytes (16 bit), rarely used
	Boolean = 0x21, // Boolean value stored on 1 byte (8 bit)
	String = 0x30, // String value
	// 0x31, // Variation of String value, probably never used
	Path = 0x32, // Path value, used for disk paths, sometimes for URLs
	Picture = 0x33, // Picture data
	CString = 0x34, // CString
	PasString = 0x35, // PasString
	Tag = 0x37, // Tag
	SubString = 0x3F, // SubString
	Array = 0x40, // Array
	// 0x41, // Array Data Ptr
	SubArray = 0x4F, // SubArray
	Cluster = 0x50, // Cluster, storing several sub-TDs in a sequence
	Variant = 0x53, // Variant
	Waveform = 0x54, // Measurement (Waveform) data; has multiple sub-types
	// 0x5E, // Complex fixed point number
	//  0x5F, // Fixed point number
	Block = 0x60, // Block
	Type_block = 0x61, // Type block
	Void_block = 0x62, // Void block
	Aligned_block = 0x63, // Aligned block
	Repeated_block = 0x64, // Repeated block, stores amount of repeats and one sub-TD to be repeated
	Alignment_marker = 0x65, // Alignment marker
	Reference = 0x70, // Refnum; has multiple sub-types, each with different set of properties
	Set = 0x73, // Set Collection
	Map = 0x74, // Map Collection
	Ptr = 0x80, // Ptr
	Ptr_to = 0x83, // Ptr to
	Ext_Data = 0x84, // Ext Data
	Array_interface = 0xA0, // Array interface
	Interface_to_data = 0xA1, // Interface to data
	Function = 0xF0, // Function
	TypeDef = 0xF1, // TypeDef
	PolyVI = 0xF2 // PolyVI
};