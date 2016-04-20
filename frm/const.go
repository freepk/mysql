package frm

const (
	decimalFieldType    = 0
	tinyFieldType       = 1
	shortFieldType      = 2
	longFieldType       = 3
	floatFieldType      = 4
	doubleFieldType     = 5
	nullFieldType       = 6
	timeStampFieldType  = 7
	longLongFieldType   = 8
	int24FieldType      = 9
	dateFieldType       = 10
	timeFieldType       = 11
	dateTimeFieldType   = 12
	yearFieldType       = 13
	newDateFieldType    = 14
	varCharFieldType    = 15
	bitFieldType        = 16
	timeStamp2FieldType = 17
	dateTime2FieldType  = 18
	time2FieldType      = 19
	newDecimalFieldType = 246
	enumFieldType       = 247
	setFieldType        = 248
	tinyBlobFieldType   = 249
	mediumBlobFieldType = 250
	longBlobFieldType   = 251
	blobFieldType       = 252
	varStringFieldType  = 253
	stringFieldType     = 254
	geomertyFieldType   = 255
)

const (
	geometryGeomType           = 0
	pointGeomType              = 1
	lineStringGeomType         = 2
	polygonGeomType            = 3
	multiPointGeomType         = 4
	multiLineStrintGeomType    = 5
	multiPolygonGeomType       = 6
	geometryCollectionGeomType = 7
)

const (
	nullableFieldFlag = 0x8000
	signedFieldFlag   = 0x0001
)

const (
	decimalShift = 8
	decimalMask  = 0x1f
)

const (
	tableFileType = 0x01fe
	viewFileType  = 0x5954
)

const (
	frmStructSize    = 64
	keyStructSize    = 8
	partStructSize   = 9
	columnStructSize = 17
)

const (
	undefinedKeyAlgo = 0
	bTreeKeyAlgo     = 1
	rTreeKeyAlgo     = 2
	hashKeyAlgo      = 3
	fullTextKeyAlgo  = 4
)

const (
	binaryCharset = 63
)

const (
	emptyString = ""
)

const (
	allowDupsKeyFlag = 0x0001
)
