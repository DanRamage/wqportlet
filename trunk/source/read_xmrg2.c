/* read_xmrg2.c

   Last modified: 2/1/2006 
   Added capability to work on either Big Endian or Little Endian machines.
   
	This program reads an xmrg file and writes to an ASCII vector file 
   with each line containing col# row# and data (in mm) starting from lower 
   left corner of the region.

   The program recognizes two types of xmrg headers; pre- and post
   AWIPS Bld 4.2 (also modified in Jan. 2000 to recognize pre-1997 
   headers which don't have a second record in the header).
   The following syntax may be used to compile and to execute on
   HP workstations.
   
   1) To create an executable; (on HPUNIX) cc -g -Aa -o read_xmrg2 read_xmrg2.c
                               (on Linux) using gcc: gcc -o read_xmrg2 read_xmrg2.c
	2) To run; read_xmrg xmrgmmddyyhhz where xmrgmmddyyhhz is an input XMRG file 
   name.  The output file produced is xmrgmmddyyhhz.out.

   The orientation of the precipitation field with respect to the
   i and j indices used in the program is as follows:

        i      j      physical location
   ------------------------------------------------------------------
        0      0      lowerleft corner of the RFC rectangle
   MAXX-1      0      lowerright corner of the RFC rectangle
        0 MAXY-1      upperleft corner of the RFC rectangle
   MAXX-1 MAXY-1      uppperright corner of the RFC rectangle

   The HRAP coordinates of the lowerleft corner of the RFC rectangle 
   is given by (IORIG,JORIG) in the XMRG file header (see below).  Note that
   the point (float(IORIG),float(JORIG)) corresponds to the lowerleft 
   corner of the HRAP box in the lowerleft corner of the RFC
   rectangle.  The lat-lon of this point can be obtained from the
   subroutine hrap_to_latlon(float(IORIG),float(JORIG),flon,flat).
   Likewise, the lat-lon of the upperright corner of the HRAP box
   in the upperright corner of the RFC rectangle is given by
   hrap_to_latlon(float(IORIG+MAXX),float(JORIG+MAXY),flon,flat). */

#include <stdio.h>
#include <math.h>
#include <stdlib.h>


main(int argc,char *argv[])

{

        FILE  *in_file_ptr,*out_file_ptr;
        char  user_id[10],date[10],time[10],process_flag[8];
        char  date2[10],time2[10];
        char  out_name[20];

        int   rfchd[4];
        int   numbytes_a[2];
		  int   numbytes;

        int   MAXX,MAXY,IORIG,JORIG;
        int   i,j;
		  signed char  fourbyte_ptr[4],end_ptr[4];
		  signed char  numbytes_ptr[4];
        short *onerow;
        float **matrix;
        float outval;
        short int reversebytes;
		  void reverse_byte_order(int *,int);
		  void reverse_byte_order_short(short *,int);
		  
        /* end variable declaration */
           
   if (argc != 2)
   {
           (void)printf("Incorrect number of arguments. Should be 2.\n");
      exit(0);
   }
        
   in_file_ptr=fopen(argv[1],"rb");
   if (in_file_ptr == NULL)
      {
      (void)printf("Can not open file %s for input.\n",argv[1]);
                return(1);
      }
   (void)strcpy(out_name,argv[1]);
   (void)strcat(out_name,".out");
   out_file_ptr=fopen(out_name,"w");

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
        
		  IORIG=rfchd[0];
        JORIG=rfchd[1];
        MAXX=rfchd[2];
        MAXY=rfchd[3];
		  
        /*echo to screen*/
        (void)printf("x-coordinate (HRAP) of the lowerleft corner of the RFC rectangle %d\n",IORIG);
        (void)printf("y-coordinate (HRAP) of the lowerleft corner of the RFC rectangle %d\n",JORIG);
        (void)printf("number of HRAP bins along the x-coordinate in the RFC rectangle %d\n",MAXX);
        (void)printf("number of HRAP bins along the y-coordinate in the RFC rectangle %d\n",MAXY);

        /*each record is preceded and followed by 4 bytes*/
        /*first record is 4+16+4 bytes*/
        fseek(in_file_ptr, 20, SEEK_SET);
        
        /*read second FORTRAN record*/
		  /*here I am reading an array with two elements instead of 1*/
		  /*because I couldn't successfully get the reverse_byte_order*/
		  /*routine to work with other approaches*/
        fread(&numbytes_a,sizeof(int),2,in_file_ptr);
        if (reversebytes)
           (void)reverse_byte_order(numbytes_a,2);
        (void)printf("numbytes %d\n",numbytes_a[1]);
		  numbytes=numbytes_a[1];
        
		  /***********************************************************
		*   account for all possible header lengths in xmrg files
		************************************************************/                                
        
        if ((int) numbytes == 66)
        {
		     /* first record (24) plus second record(74=66+8) is 98*/
           fseek(in_file_ptr, 98, SEEK_SET);
           
			  /*(void)printf("user_id %10s\n",user_id);
           (void)printf("date %10s\n",date);
           (void)printf("time %10s\n",time);
           (void)printf("process_flag %8s\n",process_flag);
           (void)printf("datelen %d\n",strlen(date));
           (void)printf("timelen %d\n",strlen(time));
           (void)printf("user_id %d\n",strlen(user_id));
           (void)printf("date2 %s\n",date2);
           (void)printf("time2 %s\n",time2);*/
			 
        }
        else if ((int) numbytes==38)
        {
           fseek(in_file_ptr, 70, SEEK_SET);
           /*(void)printf("user_id %10s\n",user_id);
           (void)printf("date %10s\n",date);
           (void)printf("time %10s\n",time);
           (void)printf("process_flag %8s\n",process_flag);*/
          
        }
        else if ((int) numbytes==37)
        {
           fseek(in_file_ptr, 69, SEEK_SET);
           (void)printf("WARNING: SECOND RECORD ONLY HAS 37 BYTES\n");
           (void)printf("SHOULD HAVE 38 BYTES\n");
           (void)printf("Assuming data is still valid. . . \n");
                  /*(void)printf("date %10s\n",date);
              (void)printf("time %10s\n",time);
              (void)printf("process_flag %8s\n",process_flag);
              (void)printf("numbytes %d\n",numbytes);*/
        }
        else if ((int) numbytes == (MAXX*2))
        {
            (void)printf("Reading pre-1997 format.\n");
            fseek(in_file_ptr,24, SEEK_SET);
        }
        else
        {  
            /*(void)printf("numbytes %d\n",numbytes);*/
            (void)printf("Error! Header file is in a nonstandard format. Data NOT READ!\n");
            exit(1);    
        }       
   
      /* allocate memory for arrays */
      onerow = (short int*) malloc(sizeof(short int*)*MAXX);
      matrix = (float**) malloc(sizeof(float*)*MAXY);
      for (j=0;j<MAXY;j++)
         matrix[j]=(float*) malloc(sizeof(float)*MAXX); 
    
      for(j=0;j<MAXY;j++) 
        {
           fseek(in_file_ptr, 4, SEEK_CUR);
           /* read one row */
           fread(onerow,sizeof(short),MAXX,in_file_ptr);
           if (reversebytes)
              (void) reverse_byte_order_short(onerow,MAXX);
           fseek(in_file_ptr, 4, SEEK_CUR);
           for(i=0;i<MAXX;i++)
           {
              outval = (float) onerow[i];
              matrix[j][i] = outval;

           } /* close i */
        } /* close j  */

        for(j=0; j<MAXY; j++)
        {
                for(i=0; i<MAXX; i++)
                {
                        outval=matrix[j][i];
                        if (matrix[j][i] < 0)
                        {
                                outval=-999.0;
                        } 
                        else
                        {
                           outval = outval/100.0;
                                /*  convert from hundredths of mm to mm*/
                        }
                        fprintf(out_file_ptr,"%d %d %f\n",i,j,outval);
                }
      }

/*free allocated memory*/
free(onerow);
for (j=0;j<MAXY;j++) 
{ 
   free(matrix[j]); 
}
free(matrix);
fclose(in_file_ptr);
fclose(out_file_ptr);

return(0);
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
