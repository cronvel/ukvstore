#!/usr/bin/env node
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

const ukvstore = require( '..' ) ;
const KVStore = ukvstore.KVStore ;
const termkit = require( 'terminal-kit' ) ;
const term = termkit.terminal ;



async function run() {
	var store , input , matches , command , key , value , history = [] ;
	
	store = new KVStore( process.argv[ 2 ] || './test.db' , { bufferValues: false } ) ;
	await store.loadDB() ;
	
	term.on( 'key' , key => {
		if ( key === 'CTRL_C' ) {
			term.green( '\nCtrl-C: quit\n' ) ;
			process.exit() ;
		}
	} ) ;
	
	for ( ;; ) {
		term( '> ' ) ;
		input = await term.inputField( { history } ).promise ;
		term( '\n' ) ;

		matches = input.match( / *([^ ]+)(?: +([^ ]+)(?: +(.+))?)? */ ) ;

		if ( ! matches ) {
			term.red( 'Syntax error\n' ) ;
			continue ;
		}
		
		history.push( input ) ;
		
		[ , command , key , value ] = matches ;
		
		switch ( command ) {
			case 'has':
				if ( ! key ) {
					term.red( 'Syntax error\n' ) ;
					break ;
				}
				
				if ( store.has( key ) ) {
					term.green( 'Having "%s": ^G^+yes\n' , key ) ;
				}
				else {
					term.green( 'Having "%s": ^R^+no\n' , key ) ;
				}
				
				break ;
			
			case 'get':
				if ( ! key ) {
					term.red( 'Syntax error\n' ) ;
					break ;
				}
				
				value = store.get( key ) ;
				
				if ( value === undefined ) {
					term.green( 'Getting "%s": <not found>\n' , key ) ;
				}
				else {
					term.green( 'Getting "%s": "%s"\n' , key , value ) ;
				}
				
				break ;
			
			case 'set':
				if ( ! key || ! value ) {
					term.red( 'Syntax error\n' ) ;
					break ;
				}
				
				term.green( 'Setting "%s" to "%s"\n' , key , value ) ;
				await store.set( key , value ) ;
				break ;

			case 'del':
			case 'delete':
				if ( ! key ) {
					term.red( 'Syntax error\n' ) ;
					break ;
				}
				
				term.green( 'Deleting "%s"\n' , key ) ;
				await store.delete( key ) ;
				break ;

			case 'clear':
				term.green( 'Clearing %i entries\n' , store.size ) ;
				await store.clear() ;
				break ;

			case 'size':
				term.green( 'Size %i\n' , store.size ) ;
				break ;

			case 'keys':
				term.green( 'Keys:\n' ) ;
				for ( let key of store.keys() ) {
					term( '  %s\n' , key ) ;
				}
				break ;

			case 'vals':
			case 'values':
				term.green( 'Values:\n' ) ;
				for ( let value of store.values() ) {
					term( '  %s\n' , value ) ;
				}
				break ;

			case 'l':
			case 'list':
			case 'entries':
				term.green( 'Entries:\n' ) ;
				for ( let entry of store.entries() ) {
					term( '  %s: %n\n' , entry[ 0 ] , entry[ 1 ] ) ;
				}
				break ;

			default:
				term.red( 'Unknown command: %s\n' , command ) ;
		}
	}
	
	process.exit() ;
}

run() ;

