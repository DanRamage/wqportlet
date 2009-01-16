/*     
Name: xmrgtoasc.c

------------------
Last Modified:  12/7/2006 !!! CHANGES !!! 
(1) Added capability to work with files created on either Big Endian
or Little Endian machines regardless of the machine on which this 
program is compiled.
(2) Changed output units to be mm rather than cm to be consistent with
the other C program provided on the DMIP2 web site referred to as 
read_xmrg2.c (www://hsp.nws.noaa.gov/oh/hrl/dmip/2/xmrgformat.html). 
------------------

Description: Read an XMRG file and write to an ASCII file that
can be read directly read by ArcView as an Arc/Info grid. Output 
coordinates are either HRAP or polar stereographic depending on the
third command line argument provided by the user.

This program will recognize two types of XMRG headers
-- pre and post AWIPS Bld 4.2
Also modified in Jan. 2000 to recognize pre-1997 headers which don't 
have a second record in the header.

Successfully compiled and run on HPUNIX using the following syntax:
cc -g -Aa -o xmrgtoasc xmrgtoasc.c

and on Red Hat Linux using gcc with the following syntax:
gcc -o xmrgtoasc xmrgtoasc.c

Syntax to run the program is then:
xmrgtoasc <infilename> <outfilename> <hrap|ster>
Third argument is either the string "hrap" or the string "ster",
depending on the desired coordinates for the output grid.  
-- Note1:  Do not include an extension in the output file name.
-- Note2:  This will not decode an XMRG file properly on a Little Endian
machine (e.g. Linux running on an Intel chip) see
http://www.nws.noaa.gov/oh/hrl/dmip/nexrad.html for mor info 

*/

#include <stdio.h>
#include <math.h>
#include <stdlib.h>

main(int argc,char *argv[])

{

	FILE     *in_file_ptr, *out_file_ptr;

	char    binfile[18], outfile[18];
	char     tempstr[256], user_id[10], date[10], time[10], process_flag[8];
	char date2[10],time2[10];
	char dummy[10],asc_name[18];

	int     rfchd[4];
   int     numbytes_a[2];
	int     ddd[2];
	int numsuccess;
   int numbytes;

   short *itest;
	long     MAXX, MAXY, XOR, YOR;
	long     nrows, ncols;
	long     i, j, temp;
	/*short     precip[1000];*/
	short  *onerow;
	/*int     rainfall[1000][1000];*/
	float **matrix;
	float outval;
	float xstereo,ystereo;
   /*variables related to machine dependent byte order*/
   short int reversebytes;
   void reverse_byte_order(int *,int);
	void reverse_byte_order_short(short *,int);
   
	/* end variable declaration */
	   
   if (argc != 4)
	{
	   (void)printf("Incorrect number of arguments.\n");
      (void)printf("Proper syntax: xmrgtoasc <infilename> <outfilename> <hrap|ster>\n");
      exit(1);
   }
	
   in_file_ptr=fopen(argv[1],"rb");
   if (in_file_ptr == NULL)
      {
      (void)printf("Can not open file %s for input.\n",argv[1]);
		return(1);
      }
   (void)strcpy(asc_name,argv[2]);
   (void)strcat(asc_name,".asc");
   out_file_ptr=fopen(asc_name,"w");
   if (out_file_ptr == NULL)
      {
      (void)printf("Can not open file %s for output.\n",argv[2]);
      return(1);
      }

   /* start reading the XMRG file*/
   /* determine if byte reversal is needed */
   fread(&numbytes,sizeof(int),1,in_file_ptr);
   if (numbytes != 16)
      reversebytes = 1;
   else
      reversebytes = 0;
   
	/*SEEK_SET specifies the position offset from the beginning of the file*/
	fseek(in_file_ptr, 4, SEEK_SET);
	for(i=0;i<4;i++)
	{
		fread(&rfchd[i], sizeof(int), 1, in_file_ptr);
	}
   
   if (reversebytes)
            (void) reverse_byte_order(rfchd,4);

	XOR=rfchd[0];
	YOR=rfchd[1];
	xstereo=XOR*4762.5-401.0*4762.5;
	ystereo=YOR*4762.5-1601.0*4762.5;
	MAXX=rfchd[2];
	MAXY=rfchd[3];
	nrows = MAXY;
	ncols = MAXX;
	
	/*print to header file*/
	(void)fprintf(out_file_ptr,"ncols %d\n",MAXX);
	(void)fprintf(out_file_ptr,"nrows %d\n",MAXY);
	/*echo to screen*/
	(void)printf("ncols %d\n",MAXX);
	(void)printf("nrows %d\n",MAXY);
	if (strcmp(argv[3],"hrap")==0) 
	{
	   (void)fprintf(out_file_ptr,"xllcorner %d\n",XOR);
	   (void)fprintf(out_file_ptr,"yllcorner %d\n",YOR);
	   (void)fprintf(out_file_ptr,"cellsize 1\n");
		(void)printf("xllcorner %d\n",XOR);
	   (void)printf("yllcorner %d\n",YOR);
	   (void)printf("cellsize 1\n");
	}
   else if (strcmp(argv[3],"ster")==0) 
	{
	   (void)fprintf(out_file_ptr,"xllcorner %f\n",xstereo);
	   (void)fprintf(out_file_ptr,"yllcorner %f\n",ystereo);
	   (void)fprintf(out_file_ptr,"cellsize 4762.5\n");
	   (void)fprintf(out_file_ptr,"nodata_value -9999.0\n");
	   (void)printf("xllcorner %f\n",xstereo);
	   (void)printf("yllcorner %f\n",ystereo);
	   (void)printf("cellsize 4762.5\n");
	   /*nodata_value and byteorder are optional*/
	   /*echo to screen*/
    }
	 else
	 {
	    (void)printf("Specify either hrap or ster as the third argument.\n");
	 }

   /*each record is preceded and followed by 4 bytes*/
	/*first record is 4+16+4 bytes*/
   /*read second FORTRAN record*/
	/*here I am reading an array with two elements instead of 1*/
   /*because I couldn't successfully get the reverse_byte_order*/
   /*routine to work with other approaches*/
   /*thus, the read starts with one integer at the end of the first record*/
	fseek(in_file_ptr, 20, SEEK_SET);
   
   fread(&numbytes_a,sizeof(int),2,in_file_ptr);
   if (reversebytes)
           (void)reverse_byte_order(numbytes_a,2);
   
   (void)printf("numbytes %d\n",numbytes_a[1]);
	numbytes=numbytes_a[1];
           
	fseek(in_file_ptr, 4, SEEK_CUR);
	
	numsuccess=fscanf(in_file_ptr, "%10s %10s %10s %8s %10s %10s", user_id, date, time, process_flag,date2,time2);
	/*numsuccess=fscanf*/ 
	
	/*first record (24) plus second record(46) is 70*/
	/*if (strlen(date2)>0)*/
	if ((int) numbytes == 66)
	{
		fseek(in_file_ptr, 98, SEEK_SET);
		(void)printf("user_id %10s\n",user_id);
	   (void)printf("date %10s\n",date);
	   (void)printf("time %10s\n",time);
	   (void)printf("process_flag %8s\n",process_flag);
	   (void)printf("datelen %d\n",strlen(date));
	   (void)printf("timelen %d\n",strlen(time));
	   (void)printf("user_id %d\n",strlen(user_id));
	   (void)printf("date2 %s\n",date2);
	   (void)printf("time2 %s\n",time2);
	   /*(void)printf("numbytes %d\n",numbytes);*/
	}
	else if ((int) numbytes==38)
	{
		fseek(in_file_ptr, 70, SEEK_SET);
		/*(void)printf("gothere\n");*/
		(void)printf("user_id %10s\n",user_id);
	   (void)printf("date %10s\n",date);
	   (void)printf("time %10s\n",time);
	   (void)printf("process_flag %8s\n",process_flag);
	   /*(void)printf("numbytes %d\n",numbytes);*/
   }
	else if ((int) numbytes==37)
	{
	   /* read first header line */
		fseek(in_file_ptr, 4, SEEK_SET);
		/* read second header line */
	   (void)printf("Reading June 1997 - Summer 1999 AWIPS format.\n");
		/*first record (24) plus second record(45) is 70*/
		fseek(in_file_ptr, 69, SEEK_SET);
		
		/*(void)printf("gothere\n");*/
		(void)printf("WARNING: SECOND RECORD ONLY HAS 37 BYTES\n");
		(void)printf("SHOULD HAVE 38 BYTES\n");
		(void)printf("Assuming data is still valid. . . \n");
		(void)printf("user_id %10s\n",user_id);
	   (void)printf("date %10s\n",date);
	   (void)printf("time %10s\n",time);
	   (void)printf("process_flag %8s\n",process_flag);
	   /*(void)printf("numbytes %d\n",numbytes);*/
   }
	else if ((int) numbytes == (ncols*2))
	/* the second record of the files was nonexistent in pre-June 1997 files.*/
	{   
		(void)printf("Reading pre-1997 format.\n");
		fseek(in_file_ptr,24, SEEK_SET);
	}
	else 
	{   
		/*(void)printf("numbytes %d\n",numbytes);*/
	   (void)printf("Header file is in a nonstandard format. Data NOT READ!\n");
		exit(1);
	}	
	
	/* allocate memory for arrays */
	onerow = (short int*) malloc(sizeof(short int*)*ncols);
	matrix = (float**) malloc(sizeof(float*)*nrows);
   for (i=0;i<nrows;i++)
      matrix[i]=(float*) malloc(sizeof(float)*ncols); 
    
	for(i=nrows-1;i>-1;i--)
	{
	fseek(in_file_ptr, 4, SEEK_CUR);
	/* read one row */
	fread(onerow,sizeof(short),ncols,in_file_ptr);
   if (reversebytes)
           (void) reverse_byte_order_short(onerow,MAXX);
	fseek(in_file_ptr, 4, SEEK_CUR);
		for(j=0;j<ncols;j++)
		{
		matrix[i][j] = (float) onerow[j];

		} /* close j */
	} /* close i  */


	for(i=0; i<nrows; i++)
	{
		for(j=0; j<ncols; j++)
		{
			/*fwrite(&rainfall[i][j], 4, 1, out_file_ptr);*/
			outval=matrix[i][j];
			if (matrix[i][j] < 0)
			{
				outval=-9999.0;
			} 
			else
			{
			   outval = outval/100.0;
				/*  convert from hundredths of mm to mm*/
		   }
         /*fwrite(&outval,4,1,out_file_ptr);*/
			fprintf(out_file_ptr,"%f ",outval);
			}
			fprintf(out_file_ptr,"\n");
	}

/*free allocated memory*/
free(onerow);
for (i=0;i<nrows;i++) 
     		{ free(matrix[i]);}
free(matrix); 
fclose(in_file_ptr);
fclose(out_file_ptr);
/*fclose(hdr_file_ptr);*/

}  /**  END OF MAIN  **/

/*******************************************************************************
* MODULE NUMBER: 1
* MODULE NAME:   pk_swap_
*
* PURPOSE:       This routine reverses the ordering of the bytes in each 4-byte
*                word of an integer array.  For example consider the following
*                4-byte word whose bytes contain the characters 'A', 'B', 
*                'C', and 'D':
*
*                  byte 1   byte 2   byte 3   byte 4 
*                  A        B        C        D
*
*                This routine will reverse the ordering of these bytes in this
*                4-byte word so that the contents of each byte of the word 
*                will appear as follows:
*
*                  byte 1   byte 2   byte 3   byte 4 
*                  D        C        B        A
*
*                The need for this routine arises from differences in
*                memory architecture across different computer platforms.
*                The two memory configurations that need to be accomodated
*                are the "Big Endian" and "Little Endian" architectures.
*
*                In the "Big Endian" architecture, the left-most byte in a
*                word is the most significant byte.  In the "Little Endian"
*                architecture, the right-most byte in a word is the most
*                significant byte.
*
*                As another example, consider a 4-byte integer which contains
*                the value 66.  On a "Big Endian" system, the binary pattern
*                in a word would appear as follows:
*
*                00000000 00000000 00000000 01000010
*
*                On a "Little Endian" system, the binary pattern would 
*                appear as follows:
*
*                01000010 00000000 00000000 00000000
*
*                This routine ensures that a GRIB2 message will be accurately
*                decoded regardless of the memory architecture of the computer
*                that it is being decoded on.
*
* ARGUMENTS:
*   TYPE           DATA TYPE   NAME         DESCRIPTION/UNITS
*   Input/Output   int *       p_data       A pointer to an array of integer
*                                           values. 
*   Input          size_t *    num_elements A pointer to a size_t value
*                                           containing the number of elements
*                                           in the p_data array.
*
* RETURNS:
*   None.
*
* APIs UTILIZED:
*   None.
*
* LOCAL DATA ELEMENTS (OPTIONAL):
*   DATA TYPE       NAME         DESCRIPTION
*   signed char *   p_d          Pointer to the current element in p_data being
*                                processed
*   signed char *   p_t          Pointer to the temporary variable containing
*                                the original value of the current element
*                                in p_data being processed.
*   int             temp         Contains the original value of the 
*                                current element in p_data being processed.
*   unsigned int    i            Loop indexing variable.
*   unsigned int    k            Loop Indexing variable.
*
* DATA FILES AND/OR DATABASE:
*   None.
*
* ERROR HANDLING:
*   None.
*
********************************************************************************/

void reverse_byte_order(int *in_array,int arraysize)
{

unsigned int   i,k;
signed char *p_data;   /*data pointer*/
signed char *p_temp;   /*temporaty data pointer */
int temp;

/*printf("before %d %d\n",input_data[0],input_data[1]);*/
p_data = (signed char *) in_array - 1;
for ( k = 0 ; k < arraysize ; k++ )
  {
     temp = *( in_array + k );
     p_temp = ( signed char * ) ( &temp ) + 4;

     for  ( i = 0 ; i < 4 ; i++ )
     {
       *(++p_data) = *(--p_temp);
     }
  }
/*printf("after %d %d\n",input_data[0],input_data[1]);*/

/*free(start_ptr);*/
}

void reverse_byte_order_short(short *in_array,int arraysize)
{

unsigned int   i,k;
signed char *p_data;   /*data pointer*/
signed char *p_temp;   /*temporaty data pointer */
short temp;

/*printf("before %d %d\n",input_data[0],input_data[1]);*/
p_data = (signed char *) in_array - 1;
for ( k = 0 ; k < arraysize ; k++ )
  {
     temp = *( in_array + k );
     p_temp = ( signed char * ) ( &temp ) + 2;

     for  ( i = 0 ; i < 2 ; i++ )
     {
       *(++p_data) = *(--p_temp);
     }
  }
/*printf("after %d %d\n",input_data[0],input_data[1]);*/

/*free(start_ptr);*/
}
