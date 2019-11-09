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



function KVStore( filePath = null ) {
	this.filePath = filePath ;
	this.file = null ;
	this.eof = null ;			// End Of File offset
	this.map = new Map() ;
	this.freeSpaces = [] ;		// Store spaces that have been freed in the middle of the file

	this.inProgress = null ;	// If a write is in progress, this is the promise for that
	this.inProgressKey = null ;	// The key being written
}

module.exports = KVStore ;



KVStore.prototype.has = function( key ) {
	return this.map.has( key ) ;
} ;



KVStore.prototype.get = function( key ) {
	var mapV = this.map.get( key ) ;
	if ( ! mapV ) { return ; }
	return mapV.v ;
} ;



KVStore.prototype.set = async function( key , value ) {
	var offset ,
		mapV = this.map.get( key ) ;

	if ( mapV ) {
		mapV.v  = value ;
		offset = await this.updateDB( mapV.o , key , value ) ;
		if ( offset !== null ) { mapV.o = offset ; }
	}
	else {
		mapV = { v: value , o: null } ;
		this.map.set( key , mapV ) ;
		offset = await this.insertDB( key , value ) ;
		if ( offset !== null ) { mapV.o = offset ; }
	}
} ;



KVStore.prototype.delete = async function( key ) {
	var offset ,
		mapV = this.map.get( key ) ;

	if ( mapV ) {
		this.map.delete( key ) ;
		await this.deleteDB( mapV.o ) ;
	}
} ;



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



KVStore.prototype.minimalBlockSize = function( size , toFlag = null ) {
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

	if ( toFlag === null ) { return blockSize ; }

	if ( powerOf2 - 4 > 31 ) {
		throw new Error( "Block size too big: " + blockSize ) ;
	}

	toFlag &= MASK_NON_SIZE ;	// reset the 6 lower bits
	toFlag |= Math.max( 0 , powerOf2 - 4 ) ;
	if ( half ) { toFlag |= FLAG_PLUS_HALF_SIZE ; }

	return toFlag ;
} ;



KVStore.prototype.entryBuffer = function( key , value ) {
	var offset ,
		keyLength = Buffer.byteLength( key ) ,
		valueLength = Buffer.byteLength( value ) ,
		largeLPS = keyLength > 255 || valueLength > 65535 ,
		entrySize = 1 + ( largeLPS ? 6 : 3 ) + keyLength + valueLength ,
		flags = this.minimalBlockSize( entrySize , 0 ) ,
		blockSize = this.extractBlockSize( flags ) ,
		//buffer = Buffer.allocUnsafe( entrySize ) ;
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
	buffer.write( value , offset , valueLength ) ;
	offset += valueLength ;

	if ( blockSize > entrySize ) {
		buffer.fill( 0 , offset ) ;
	}

	return buffer ;
} ;



// Open the DB file
KVStore.prototype.open = async function() {
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

	if ( ! this.file ) { await this.open() ; }

	var offset = 0 , subOffset ,
		flags , blockSize ,
		key , value , keyLength , valueLength ,
		blockBuffer = Buffer.allocUnsafe( 16 ) ;

	console.log( "eof" , this.eof ) ;

	while ( offset < this.eof ) {
		await this.file.read( blockBuffer , 0 , 1 , offset ) ;
		flags = blockBuffer.readUInt8() ;
		console.log( "flags" , flags ) ;
		blockSize = this.extractBlockSize( flags ) ;

		if ( ! ( flags & FLAG_FREE_BLOCK ) ) {
			if ( blockBuffer.length < blockSize ) {
				blockBuffer = Buffer.allocUnsafe( blockSize ) ;
				console.log( "allocated a new buffer of size:" , blockSize ) ;
			}

			await this.file.read( blockBuffer , 0 , blockSize , offset ) ;
			console.log( "block" , blockBuffer ) ;

			subOffset = 1 ;

			if ( flags & FLAG_LARGE_LPS ) {
				keyLength = blockBuffer.readUInt16BE( subOffset ) ;
				valueLength = blockBuffer.readUInt32BE( subOffset + 2 ) ;
				subOffset += 6 ;
				console.log( "large LPS keyLength:" , keyLength , "valueLength:" , valueLength ) ;
			}
			else {
				keyLength = blockBuffer.readUInt8( subOffset ) ;
				valueLength = blockBuffer.readUInt16BE( subOffset + 1 ) ;
				subOffset += 3 ;
				console.log( "small LPS keyLength:" , keyLength , "valueLength:" , valueLength ) ;
			}

			key = blockBuffer.toString( 'utf8' , subOffset , subOffset + keyLength ) ;
			subOffset += keyLength ;
			value = blockBuffer.toString( 'utf8' , subOffset , subOffset + valueLength ) ;
			//subOffset += valueLength ;

			console.log( ">>> found key:" , key , "value:" , value ) ;
			this.map.set( key , { v: value , o: offset } ) ;
		}

		offset += blockSize ;
	}

	this.inProgress.resolve() ;
	this.inProgress = null ;
} ;



KVStore.prototype.insertDB = async function( key , value ) {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.open() ; }

	console.log( "EOF:" , this.eof ) ;

	var entryBuffer = this.entryBuffer( key , value ) ;
	console.log( entryBuffer ) ;

	await this.file.write( entryBuffer , 0 , entryBuffer.length , this.eof ) ;
	this.eof += entryBuffer.length ;

	this.inProgress.resolve() ;
	this.inProgress = null ;
} ;



KVStore.prototype.updateDB = async function( offset , key , value ) {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.open() ; }

} ;



KVStore.prototype.deleteDB = async function( offset , key , value ) {
	if ( ! this.filePath ) { return null ; }

	while ( this.inProgress ) { await this.inProgress ; }

	this.inProgress = new Promise() ;

	if ( ! this.file ) { await this.open() ; }

} ;

