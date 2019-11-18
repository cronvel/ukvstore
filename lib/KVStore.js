/*
	Micro KV Store

	Copyright (c) 2019 Cédric Ronvel

	The MIT License (MIT)

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

"use strict" ;



const fsPromise = require( 'fs' ).promises ;
const Promise = require( 'seventh' ) ;



function KVStore( filePath = null , options = {} ) {
	this.filePath = filePath ;
	this.file = null ;
	this.eof = null ;			// End Of File offset
	this.map = new Map() ;
	this.freeBlocks = {} ;		// Store spaces that have been freed in the middle of the file

	// if true, values are buffer (default: string)
	this.bufferValues = !! options.bufferValues ;
	
	// if true, values are stored in memory (the default)
	this.inMemoryValues = options.inMemoryValues !== undefined ? !! options.inMemoryValues : true ;

	this.inProgress = null ;	// If a write is in progress, this is the promise for that
	this.inProgressKey = null ;	// The key being written

	Object.defineProperties( this , {
		size: {
			get: function() { return this.map.size ; }
		}
	} ) ;
}

module.exports = KVStore ;



KVStore.prototype.has = function( key ) {
	return this.map.has( key ) ;
} ;



KVStore.prototype.get = function( key ) {
	var mapV = this.map.get( key ) ;
	
	if ( this.inMemoryValues ) {
		if ( ! mapV ) { return ; }
		return mapV.v ;
	}
	else {
		if ( ! mapV ) { return Promise.resolved ; }
		return this.retrieveDB( key , mapV ) ;
	}
} ;



KVStore.prototype.set = async function( key , value ) {
	var mapV = this.map.get( key ) ;

	if ( Buffer.isBuffer( value ) ) {
		if ( ! this.bufferValues ) {
			value = value.toString() ;
		}
	}
	else {
		if ( typeof value !== 'string' ) { value = '' + value ; }

		if ( this.bufferValues ) {
			value = Buffer.from( value ) ;
		}
	}

	if ( mapV ) {
		if ( this.inMemoryValues ) { mapV.v  = value ; }
		await this.updateDB( key , mapV , value ) ;
	}
	else {
		mapV = this.inMemoryValues ? { v: value , o: null } : { o: null } ;
		this.map.set( key , mapV ) ;
		await this.insertDB( key , mapV , value ) ;
	}
} ;



KVStore.prototype.delete = async function( key ) {
	var offset ,
		mapV = this.map.get( key ) ;

	if ( mapV ) {
		this.map.delete( key ) ;
		await this.deleteDB( key , mapV ) ;
	}
} ;



KVStore.prototype.clear = async function() {
	this.map.clear() ;
	await this.clearDB() ;
} ;



KVStore.prototype.keys = function() {
	return this.map.keys() ;
} ;



KVStore.prototype.forEach = function( fn ) {
	var key , mapV ;

	if ( this.inMemoryValues ) {
		for ( [ key , mapV ] of this.map.entries() ) {
			fn( mapV.v , key ) ;
		}
	}
	else {
		for ( [ key , mapV ] of this.map.entries() ) {
			fn( this.retrieveDB( key , mapV ) , key ) ;
		}
	}
} ;



// Call fn with a non-promise value, fn itself can be async, in that case fn calls are serialized
KVStore.prototype.asyncForEach = async function( fn ) {
	var key , mapV ;

	if ( this.inMemoryValues ) {
		for ( [ key , mapV ] of this.map.entries() ) {
			await fn( mapV.v , key ) ;
		}
	}
	else {
		for ( [ key , mapV ] of this.map.entries() ) {
			await fn( await this.retrieveDB( key , mapV ) , key ) ;
		}
	}
} ;



KVStore.prototype.values = function *() {
	if ( this.inMemoryValues ) {
		for ( let mapV of this.map.values() ) {
			yield mapV.v ;
		}
	}
	else {
		for ( let entry of this.map.entries() ) {
			yield this.retrieveDB( entry[ 0 ] , entry[ 1 ] ) ;
		}
	}
} ;



KVStore.prototype.entries = function *() {
	if ( this.inMemoryValues ) {
		for ( let entry of this.map.entries() ) {
			entry[ 1 ] = entry[ 1 ].v ;
			yield entry ;
		}
	}
	else {
		for ( let entry of this.map.entries() ) {
			entry[ 1 ] = this.retrieveDB( entry[ 0 ] , entry[ 1 ] ) ;
			yield entry ;
		}
	}
} ;



// DB part



/*
	Entry structure:
	Flags (1B) - Key LPS (1-2B) - Value LPS (2-4B) - Key - Value

	Flags structure:
	1bit: free block
	1bit: large LPS
	1bit: +50% block size
	5bits: 16 * 2^n block size
*/

const FLAG_FREE_BLOCK = 128 ;
const FLAG_LARGE_LPS = 64 ;
const MASK_NON_SIZE = FLAG_FREE_BLOCK | FLAG_LARGE_LPS ;
const FLAG_PLUS_HALF_SIZE = 32 ;
const MASK_POWER_OF_2_SIZE = 31 ;
const MASK_SIZE = FLAG_PLUS_HALF_SIZE | MASK_POWER_OF_2_SIZE ;

KVStore.prototype.extractBlockSize = flags => 2 ** ( flags & MASK_POWER_OF_2_SIZE ) * ( flags & FLAG_PLUS_HALF_SIZE ? 24 : 16 ) ;



KVStore.prototype.blockSizeToFlags =
KVStore.prototype.blockSize = function( size , toFlags = null ) {
	var powerOf2 , powerOf2AndHalf , blockSize , half = false ;

	powerOf2 = Math.ceil( Math.log2( size ) ) ;
	powerOf2AndHalf = Math.ceil( Math.log2( size * 2 / 3 ) ) ;

	if ( powerOf2 === powerOf2AndHalf ) {
		blockSize = 2 ** powerOf2 ;
	}
	else {
		powerOf2 = powerOf2AndHalf ;
		blockSize = ( 2 ** powerOf2 ) * 1.5 ;
		half = true ;
	}

	if ( powerOf2 < 4 ) {
		powerOf2 = 4 ;
		half = false ;
		blockSize = 16 ;
	}

	if ( toFlags === null ) { return blockSize ; }

	if ( powerOf2 - 4 > 31 ) {
		throw new Error( "Block size too big: " + blockSize ) ;
	}

	toFlags &= MASK_NON_SIZE ;	// reset the 6 lower bits
	toFlags |= Math.max( 0 , powerOf2 - 4 ) ;
	if ( half ) { toFlags |= FLAG_PLUS_HALF_SIZE ; }

	return toFlags ;
} ;



const INSERT_SIZE_OPTIMIZATION = 1.2 ;

KVStore.prototype.entryBuffer = function( key , value , existingSize = 0 , mapVToPopulate ) {
	var offset , flags , blockSize , buffer ,
		valueIsBuffer = Buffer.isBuffer( value ) ,
		keyLength = Buffer.byteLength( key ) ,
		valueLength = valueIsBuffer ? value.length : Buffer.byteLength( value ) ,
		largeLPS = keyLength > 255 || valueLength > 65535 ,
		entrySize = 1 + ( largeLPS ? 6 : 3 ) + keyLength + valueLength ;

	if ( entrySize <= existingSize ) {
		blockSize = existingSize ;
		flags = this.blockSizeToFlags( blockSize , 0 ) ;
	}
	else {
		// If there is no existingSize, this is an insert, and we have to add a small free space for an entry to grow,
		// if it has an entrySize bigger than current size, we have to move to another location and still allow
		// some space to grow...
		flags = this.blockSizeToFlags( entrySize * INSERT_SIZE_OPTIMIZATION , 0 ) ;
		blockSize = this.extractBlockSize( flags ) ;
	}

	buffer = Buffer.allocUnsafe( blockSize ) ;

	if ( largeLPS ) { flags |= FLAG_LARGE_LPS ; }

	buffer.writeUInt8( flags , 0 ) ;

	if ( largeLPS ) {
		buffer.writeUInt16BE( keyLength , 1 ) ;
		buffer.writeUInt32BE( valueLength , 3 ) ;
		offset = 7 ;
	}
	else {
		buffer.writeUInt8( keyLength , 1 ) ;
		buffer.writeUInt16BE( valueLength , 2 ) ;
		offset = 4 ;
	}

	buffer.write( key , offset , keyLength ) ;
	offset += keyLength ;

	if ( valueIsBuffer ) { value.copy( buffer , offset ) ; }
	else { buffer.write( value , offset , valueLength ) ; }

	if ( mapVToPopulate ) {
		// = inMemoryValues is false
		mapVToPopulate.vo = offset ;
		mapVToPopulate.vs = valueLength ;
	}
	
	offset += valueLength ;

	// Fill with NUL the remaining bytes of the block, avoid having older data (maybe sensitive) remaining
	if ( blockSize > entrySize ) { buffer.fill( 0 , offset ) ; }

	return buffer ;
} ;



// Open the DB file
KVStore.prototype.openDB = async function() {
	if ( ! this.filePath ) { return null ; }

	try {
		this.file = await fsPromise.open( this.filePath , 'r+' ) ;
		let stats = await this.file.stat() ;
		this.eof = stats.size ;
	}
	catch ( error ) {
		this.file = await fsPromise.open( this.filePath , 'w+' ) ;
		this.eof = 0 ;
	}
} ;



// Load the whole DB in memory
KVStore.prototype.loadDB = async function() {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.openDB() ; }

	var offset = 0 , valueOffset , subOffset ,
		flags , blockSize ,
		key , value , keyLength , valueLength , remainingReadLength ,
		oldBlockBuffer , newBufferSize ,
		blockBuffer = Buffer.allocUnsafe( 16 ) ;

	console.log( "Load file, EOF:" , this.eof ) ;

	while ( offset < this.eof ) {
		// Read 7 bytes: the flags + the 2 LPS
		await this.file.read( blockBuffer , 0 , 7 , offset ) ;
		flags = blockBuffer.readUInt8() ;
		console.log( "--- New Block ---" ) ;
		blockSize = this.extractBlockSize( flags ) ;

		if ( flags & FLAG_FREE_BLOCK ) {
			console.log( "  *** free block, offset:" , offset , "size:" , blockSize , "flags:" , flags ) ;
			if ( ! this.freeBlocks[ blockSize ] ) { this.freeBlocks[ blockSize ] = [] ; }
			this.freeBlocks[ blockSize ].push( offset ) ;
		}
		else {
			console.log( "  offset:" , offset , "size:" , blockSize , "flags:" , flags , flags & FLAG_LARGE_LPS ? "large LPS" : "small LPS" ) ;
			console.log( "  block" , blockBuffer ) ;

			if ( flags & FLAG_LARGE_LPS ) {
				keyLength = blockBuffer.readUInt16BE( 1 ) ;
				valueLength = blockBuffer.readUInt32BE( 3 ) ;
				subOffset = 7 ;
				remainingReadLength = 0 ;
				console.log( "  large LPS keyLength:" , keyLength , "valueLength:" , valueLength ) ;
			}
			else {
				keyLength = blockBuffer.readUInt8( 1 ) ;
				valueLength = blockBuffer.readUInt16BE( 2 ) ;
				subOffset = 4 ;
				remainingReadLength = -3 ;
				console.log( "  small LPS keyLength:" , keyLength , "valueLength:" , valueLength ) ;
			}

			if ( this.inMemoryValues ) {
				remainingReadLength += keyLength + valueLength ;
			}
			else {
				remainingReadLength += keyLength ;
			}
			
			if ( blockBuffer.length < 7 + remainingReadLength ) {
				// Use .blockSize() to avoid allocating more and more buffer everytime it is increased by 1
				newBufferSize = this.blockSize( 7 + remainingReadLength ) ;
				oldBlockBuffer = blockBuffer ;
				blockBuffer = Buffer.allocUnsafe( newBufferSize ) ;
				
				// Don't forget to copy back the first 7 bytes
				oldBlockBuffer.copy( blockBuffer , 0 , 0 , 7 ) ;
				console.log( "  !! allocated a new buffer of size:" , newBufferSize ) ;
			}

			console.log( "  remainingReadLength:", remainingReadLength , "offset + 7:", offset + 7 ) ;
			if ( remainingReadLength > 0 ) {
				// Read the block, but don't read again the flags + LPS bytes
				await this.file.read( blockBuffer , 7 , remainingReadLength , offset + 7 ) ;
			}
			
			console.log( "  block" , blockBuffer ) ;
			console.log( "  partial block" , blockBuffer.slice( 0 , 7 + remainingReadLength ) ) ;
			key = blockBuffer.toString( 'utf8' , subOffset , subOffset + keyLength ) ;
			subOffset += keyLength ;

			if ( this.inMemoryValues ) {
				if ( this.bufferValues ) {
					// This is the correct way to slice+copy, there is no dedicated API
					// https://nodejs.org/dist/latest-v12.x/docs/api/buffer.html#buffer_buf_slice_start_end
					value = Uint8Array.prototype.slice.call( blockBuffer , subOffset , subOffset + valueLength ) ;
				}
				else {
					value = blockBuffer.toString( 'utf8' , subOffset , subOffset + valueLength ) ;
				}

				console.log( "  >>> key:" , key , "; value:" , value ) ;
			
				this.map.set( key , { v: value , o: offset , s: blockSize } ) ;
			}
			else {
				console.log( "  >>> key:" , key ) ;
				// For faster read, we need to save the value offset and valueLength to avoid reading
				// the flags and both LPS before doing the actual value read (1 I/O instead of 2 I/O)
				this.map.set( key , { o: offset , s: blockSize , vo: subOffset , vs: valueLength } ) ;
			}
		}

		offset += blockSize ;
	}

	this.inProgress.resolve() ;
	this.inProgress = null ;
} ;



// Only used when .inMemoryValues is false
KVStore.prototype.retrieveDB = async function( key , mapV ) {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.openDB() ; }


	var value = Buffer.allocUnsafe( mapV.vs ) ;
	await this.file.read( value , 0 , mapV.vs , mapV.o + mapV.vo ) ;
	
	if ( ! this.bufferValues ) {
		value = value.toString( 'utf8' ) ;
	}

	//console.log( "  >>> retrieve key:" , key , "; value:" , value ) ;

	this.inProgress.resolve() ;
	this.inProgress = null ;

	return value ;
} ;



KVStore.prototype.insertDB = async function( key , mapV , value ) {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.openDB() ; }

	var entryBuffer = this.entryBuffer( key , value , undefined , ! this.inMemoryValues && mapV ) ;
	console.log( entryBuffer ) ;

	await this.insertEntryBuffer( entryBuffer , mapV ) ;
	
	this.inProgress.resolve() ;
	this.inProgress = null ;
} ;



KVStore.prototype.insertEntryBuffer = async function( entryBuffer , mapV ) {
	if ( this.freeBlocks[ entryBuffer.length ] && this.freeBlocks[ entryBuffer.length ].length ) {
		// There is a free block, use it!
		mapV.o = this.freeBlocks[ entryBuffer.length ].pop() ;
		console.log( "re-use a free-block at offset:" , mapV.o ) ;
	}
	else {
		// Append it at the end of the file
		mapV.o = this.eof ;
		this.eof += entryBuffer.length ;
		console.log( "insert at the end of the file:" , mapV.o ) ;
	}

	mapV.s = entryBuffer.length ;
	await this.file.write( entryBuffer , 0 , entryBuffer.length , mapV.o ) ;
} ;



KVStore.prototype.deleteDB = async function( key , mapV ) {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.openDB() ; }

	await this.clearBlock( mapV ) ;

	this.inProgress.resolve() ;
	this.inProgress = null ;
} ;



KVStore.prototype.clearBlock = async function( mapV ) {
	var clearBuffer = Buffer.alloc( mapV.s ) ,
		clearFlags = this.blockSizeToFlags( mapV.s , FLAG_FREE_BLOCK ) ;

	clearBuffer.writeUInt8( clearFlags ) ;
	console.log( "clear flags:" , clearFlags , "size:" , mapV.s ) ;

	await this.file.write( clearBuffer , 0 , clearBuffer.length , mapV.o ) ;

	// Add that block to the list of free blocks
	if ( ! this.freeBlocks[ mapV.s ] ) { this.freeBlocks[ mapV.s ] = [] ; }
	this.freeBlocks[ mapV.s ].push( mapV.o ) ;
} ;



KVStore.prototype.updateDB = async function( key , mapV , value ) {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.openDB() ; }

	var entryBuffer = this.entryBuffer( key , value , mapV.s , ! this.inMemoryValues && mapV ) ;
	console.log( entryBuffer ) ;

	if ( entryBuffer.length > mapV.s ) {
		// Re-allocate the block
		await this.clearBlock( mapV ) ;
		await this.insertEntryBuffer( entryBuffer , mapV ) ;
	}
	else {
		mapV.s = entryBuffer.length ;
		await this.file.write( entryBuffer , 0 , entryBuffer.length , mapV.o ) ;
	}

	this.inProgress.resolve() ;
	this.inProgress = null ;
} ;



KVStore.prototype.clearDB = async function() {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.openDB() ; }

	await this.file.truncate() ;
	this.eof = 0 ;
	this.freeBlocks = {} ;		// No more free blocks

	this.inProgress.resolve() ;
	this.inProgress = null ;
} ;

