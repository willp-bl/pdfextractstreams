/*
 * Copyright 2013 The British Library / The SCAPE Project Consortium
 * Author: William Palmer (William.Palmer@bl.uk)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package uk.bl.dpt.pdfextractstreams;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.Inflater;

public class PDFExtractStreams {

	public class Stream {
		String gObjinfo;
		long gStart;
		long gLength;
		public Stream(String pObjinfo, long pStart, long pLength) {
			gObjinfo = pObjinfo;
			gStart = pStart;
			gLength = pLength;
		}
	}
	
	private Map<String, String> pdfDirectory = new HashMap<String, String>();
	private List<Stream> streams = new LinkedList<Stream>();
	
	/**
	 * Deflate a file
	 * @param inf in filename
	 * @param out out filename
	 * @throws IOException
	 */
	private void deflateStream(String inf, String out) throws IOException {
		try {
			File test = new File(inf);
		     // Decompress the bytes
			FileInputStream fr = new FileInputStream(test);
			byte[] in = new byte[(int) test.length()];
			fr.read(in);
			fr.close();
			FileOutputStream fos = new FileOutputStream(out);//test.getName()+".deflate");
		     Inflater decompresser = new Inflater();
		     decompresser.setInput(in, 0, in.length);
		     byte[] buf = new byte[32768];
				int bytesRead = 0;
				while(!decompresser.finished()) {
					bytesRead = decompresser.inflate(buf);
					fos.write(buf, 0, bytesRead);
				}
			fos.close();
		     decompresser.end();
		 } catch(java.io.UnsupportedEncodingException ex) {
		     // handle
		 } catch (java.util.zip.DataFormatException ex) {
		     // handle
		 }
	}
	
	/**
	 * Copy an output stream (also deflate it and try and identify content type)
	 * @param in input file
	 * @param out output file
	 * @param offset offset in input file
	 * @param size size of output file
	 * @throws IOException
	 */
	private File copyStream(String in, String out, long offset, long size) throws IOException {
		File o = new File(out);
		if(!o.exists()) {
			//	System.out.println("copying from local fs");
			FileInputStream fis = new FileInputStream(in);
			FileOutputStream fos = new FileOutputStream(o);
			byte[] buffer = new byte[32768];
			fis.skip(offset);
			int bytesRead = 0;
			long count = 0;
			boolean deflate = false;
			boolean deflatecheck = false;
			while(/*fis.available()>0&&*/count<size) {
				bytesRead = fis.read(buffer);
				if(!deflatecheck) {
					if(buffer[0]==120&&buffer[1]==-100) {
						deflate = true;
					}
					deflatecheck = true;
				}
				if(!(bytesRead+count<size)) {
					bytesRead = (int)(size-count);
				}
				fos.write(buffer, 0, bytesRead);
				count += bytesRead;
			}
			fis.close();
			fos.close();
			String newOut = out;
			if(deflate) {
				newOut += ".deflate";
				deflateStream(out, newOut);
				o.delete();
				o = new File(newOut);
			}
			String ext = FormatDetector.getExt(new File(newOut)).trim();
			if(ext!=null) {
				if(ext.equals("plain")) ext = "txt";
				File n = new File(newOut+"."+ext);
				//we need to run garbage collector here so that we can rename the file!?
				System.gc();
				o.renameTo(n);
				System.out.println(o+" -> "+n);
				return n;
			}
			return new File(newOut);
		}
		return null;//file exists
	}

	/**
	 * Keep reading until match is found
	 * @param match text to match
	 * @param buf buffer to read from
	 * @return matched string
	 */
	private String chompUntil(String match, PositionReader buf) {
		String endstream = "";
		//int count = 0;
		while(true) {
			char next = buf.nextChar();
			//count++;
			String potential = endstream+next;
			if(potential.equals(match.substring(0, potential.length()))) {
				endstream = endstream+next;
				//on the right track
				if(endstream.length()==match.length()) {
					//we have a match for "match"
					break;
				}
			} else {
				endstream = "";
			}						
		}
		//System.out.println("Chomped: "+count);
		return endstream;
	}
	
	/**
	 * Extract the directory of streams and other objects
	 * @param file pdf file to read
	 * @throws IOException
	 */
	private void extractDirectory(String file) {
		PositionReader buf = new PositionReader(file);
		String line = buf.readLine();
		if(!line.toUpperCase().startsWith("%PDF")) return; 
		while(buf.ready()) {
			line = buf.readLine().trim();
			if(line.endsWith("obj")) {
				//System.out.println("obj @ "+buf.getPos()+" line: "+line);
				//start of object
				//read << until >> \n endobj
				String objinfo = "";
				boolean endLoop = false;
				int bracketDepth = 0;
				char c = '\0';
				while(!endLoop) {
					c = buf.nextChar();
					while(true) {
						//chomp whitespace
						if(c!='\n') {
							if(c!='\r') {
										break;
							}
						}
						c = buf.nextChar();
					}
					
					//start of a new bracket
					if(c=='<'&&buf.peekNextChar()=='<') {
						bracketDepth++;
						buf.nextChar();
						objinfo += "<< ";
						continue;
					}
					if(c=='>'&&buf.peekNextChar()=='>') {
						bracketDepth--;
						buf.nextChar();
						objinfo += ">> ";

						if(bracketDepth==0) {
							//end of object
							int count = 0;
							char nextChar = buf.peekNextChar();
							//i.e. endobj or stream
							while(true) {
								if(nextChar=='e') {
									endLoop = true;
									break;//endobj
								}
								if(nextChar=='s') {
									endLoop = true;
									break;//stream
								}	
								//if we shouldn't skip the char then add it to the string
								char next = buf.nextChar();
								if(next!='\n') {
									if(next!='\r') {
										objinfo += next;
									}
								}
								nextChar = buf.peekNextChar();
								count++;
								if(count>300) System.exit(-1);
							}
							if(endLoop) objinfo += buf.readLine();						
							break;
						}
					}
					
					if(c=='e'&buf.peekNextChar()=='n') {
						//endobj?
						char[] temp = new char[5];
						for(int j=0;j<temp.length;j++) {
							temp[j] = buf.nextChar();
						}
						if(temp[0]=='n'&
								   temp[1]=='d'&
								   temp[2]=='o'&
								   temp[3]=='b'&
								   temp[4]=='j') {
							objinfo += "endobj";
							endLoop = true;
						} else {
							objinfo += c;
							for(int j=0;j<temp.length;j++) {
								objinfo+=temp[j];
							}
						}
						continue;
					}
					
					if(c!='\n') {
						if(c!='\r') {
							objinfo += c;
						}
					}
										
				}
				
				if(objinfo.contains("stream")) {
					System.out.println("    Stream obj: "+line+" "+objinfo);

					streams.add(new Stream(line+" "+objinfo, buf.getPos(), -1));

					@SuppressWarnings("unused")
					String endstream = chompUntil("endstream", buf);

					if(!buf.readLine().equals("endobj")) {
						System.err.println("ERROR??? No endobj?");
					}
					
				} else {
					if(!objinfo.trim().endsWith("endobj")) { 
						//this probably shouldn't happen
						System.out.println("Chomping");
						chompUntil("endobj", buf);
					}
					objinfo = line+" "+objinfo;//+" "+s;
					System.out.println("Non-stream obj: "+objinfo);//
					//add to pdfDirectory here
					pdfDirectory.put(objinfo.substring(0, objinfo.indexOf("obj")).trim(), objinfo.substring(objinfo.indexOf("obj")+3, objinfo.lastIndexOf("endobj")).trim());
				}
			} else {
				if(line.equals("xref")) {
					//ignore xref entries for now 
					chompUntil("%%EOF", buf);
				} else {
					System.out.println("non-obj/xref? @~"+buf.getPos()+"\""+line+"\"");
				}
			}

		}
		
		buf.close();
	}
	
	private void process(String file) {
		extractDirectory(file);
	
		updateReferences(file);
		
		extractAllFiles(file);
		
	}

	private void extractAllFiles(String file) {
		int i = 1;
		for(Stream stream:streams) {
			try {
				System.out.println("copying #"+i+" @"+stream.gStart+" len: "+stream.gLength);
				copyStream(file, file+"."+(i++), stream.gStart, stream.gLength);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void updateReferences(String file) {
		for(Stream stream:streams) {
			String lenKey = "/Length "; 
			int lenIndex = stream.gObjinfo.indexOf(lenKey);
			//try different combinations to find the value for the length attribute 
			int end = stream.gObjinfo.indexOf('/', lenIndex+1);
			if(end<0) end = stream.gObjinfo.indexOf('/', lenIndex+1);
			if(end<0) end = stream.gObjinfo.indexOf('>', lenIndex+1);
			//recover the length attribute
			String lenValue = stream.gObjinfo.substring(lenIndex+lenKey.length(), end).trim();
			if(lenValue.endsWith("R")) {
				//this is a reference to a size object (eugh) - it only contains the size!!!
				//we will just store the objinfo here and reconcile it against the pdfDirectory
				//later as this may contain forward references
				long actualLength = new Integer(pdfDirectory.get(lenValue.substring(0, lenValue.lastIndexOf("R")).trim()));
				stream.gLength = actualLength;
			} else {
				stream.gLength = new Integer(lenValue); 
			}
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		PDFExtractStreams pdfe = null;
		for(String s:args)
			if(new File(s).exists()) {
				pdfe = new PDFExtractStreams();
				pdfe.process(s);
			}
		}
	}

}
