 /*     
Name: xmrgtolist.c

Description: Read an XMRG file and write an ASCII file
that can be loaded into ArcView as a table and joined
to an existing 

Syntax used to compile this program on the NHDRs is as follows:
cc -g -Aa -o xmrgtolist xmrgtolist.c


*/

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#define HRAPCELLSIZE 4762.5

main(int argc,char *argv[])

{

	FILE     *in_file_ptr, *out_file_ptr,*hdr_file_ptr;

	char    binfile[18], outfile[18];
	char     tempstr[256], user_id[10], date[10], time[10], process_flag[8];
	char date2[10],time2[10];
	char hdr_name[18],dummy[10],asc_name[18];

	int     rfchd[4];
	int     ddd[2];
	int numsuccess,*numbytes;
	int iscale;

   short *itest;
	long     MAXX, MAXY, XOR, YOR;
	long     nrows, ncols;
	long     i, j, temp;
	long 	reccount;
	/*short     precip[1000];*/
	short  *onerow;
	/*int     rainfall[1000][1000];*/
	float **matrix;
	float outval,XORf,YORf;
	float bnd[2];
	float sterxll,steryll;
	int det[2];
	int nbytes;
	short nonxmrg;
	short int **flat_a,**down_a,**up_a;
   
	/* end variable declaration */
	   
   if (argc != 3)
	{
      (void) printf("Incorrect number of arguments.\n");
	   exit(0);
   }
   in_file_ptr=fopen(argv[1],"rb");
   if (in_file_ptr == NULL)
      {
      (void)printf("Can not open file %s for input.\n",argv[1]);
		return(1);
      }
   (void)strcpy(asc_name,argv[2]);
   (void)strcat(asc_name,".txt");
   out_file_ptr=fopen(asc_name,"w");
   if (out_file_ptr == NULL)
      {
      (void)printf("Can not open file %s for output.\n",argv[2]);
      return(1);
      }
   
   /* start reading the XMRG file*/
	/*SEEK_SET specifies the position offset from the beginning of the file*/
	fseek(in_file_ptr, 4, SEEK_SET);
	for(i=0;i<4;i++)
	{
		fread(&rfchd[i], sizeof(int), 1, in_file_ptr);
	}

	XOR=rfchd[0];
	YOR=rfchd[1];
	MAXX=rfchd[2];
	MAXY=rfchd[3];
	nrows = MAXY;
	ncols = MAXX;
	
	/*print to header file*/
	(void)fprintf(hdr_file_ptr,"ncols %d\n",MAXX);
	(void)fprintf(hdr_file_ptr,"nrows %d\n",MAXY);
	(void)fprintf(hdr_file_ptr,"xllcorner %d\n",XOR);
	(void)fprintf(hdr_file_ptr,"yllcorner %d\n",YOR);
	(void)fprintf(hdr_file_ptr,"cellsize 1\n");
	(void)fprintf(hdr_file_ptr,"nodata_value -9999.0\n");
	/*nodata_value and byteorder are optional*/
	/*print to screen*/
	(void)printf("ncols %d\n",MAXX);
	(void)printf("nrows %d\n",MAXY);
	(void)printf("xllcorner %d\n",XOR);
	(void)printf("yllcorner %d\n",YOR);
	(void)printf("cellsize 1\n");

   /*each record is preceded and followed by 4 bytes*/
	/*first record is 4+16+4 bytes*/
	fseek(in_file_ptr, 24, SEEK_SET);
	/*read second FORTRAN record*/
	fread(&numbytes,4,1,in_file_ptr);
	
	numsuccess=fscanf(in_file_ptr, "%10s %10s %10s %8s %10s %10s", user_id, date, time, process_flag,date2,time2);
	/*numsuccess=fscanf*/ 
	
	if ((int) numbytes == 66)
	{
	   (void)printf("Reading post summer of 1999 files");
		/* read first header line */
		fseek(in_file_ptr, 4, SEEK_SET);
	   for(i=0;i<4;i++)
	   {
		   fread(&rfchd[i], sizeof(int), 1, in_file_ptr);
	   }
		iscale=100;
		
		/* read second header line */
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
	   (void)printf("numbytes %d\n",numbytes);
	}
	/* the second record is 38 bytes for June 1997 - summer 1999 AWIPS files */
	else if ((int) numbytes==38)
	{
	   /* read first header line */
		fseek(in_file_ptr, 4, SEEK_SET);
	   for(i=0;i<4;i++)
	   {
		   fread(&rfchd[i], sizeof(int), 1, in_file_ptr);
	   }
		iscale=100;
		
		/* read second header line */
	   (void)printf("Reading June 1997 - Summer 1999 AWIPS format.");
		/*first record (24) plus second record(46) is 70*/
		fseek(in_file_ptr, 70, SEEK_SET);
		/*(void)printf("gothere\n");*/
		(void)printf("user_id %10s\n",user_id);
	   (void)printf("date %10s\n",date);
	   (void)printf("time %10s\n",time);
	   (void)printf("process_flag %8s\n",process_flag);
	   (void)printf("numbytes %d\n",numbytes);
   }
	else if ((int) numbytes==37)
	{
	   /* read first header line */
		fseek(in_file_ptr, 4, SEEK_SET);
	   for(i=0;i<4;i++)
	   {
		   fread(&rfchd[i], sizeof(int), 1, in_file_ptr);
	   }
		iscale=100;
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
	   (void)printf("numbytes %d\n",numbytes);
   }
	/* if there are only 4 bytes in the second record, this is not 
	XMRG but an HRAP cell parameter file */
	else if ((int) numbytes==8)
	/*this allows the second header line to specify the number of bytes*/
	{
	   /* read first header line */
		fseek(in_file_ptr, 4, SEEK_SET);
		for(i=0;i<2;i++)
	   {
		   fread(&bnd[i], sizeof(float), 1, in_file_ptr);
			printf("bnd %f\n",bnd[i]);
	   }
		for(i=0;i<2;i++)
	   {
		   fread(&det[i], sizeof(int), 1, in_file_ptr);
	   }
		nonxmrg=1;
		
		/* read second header line */
		
		fseek(in_file_ptr, 28, SEEK_SET);
		fread(&iscale,sizeof(int),1,in_file_ptr);
	   (void)printf("Reading cell parameter file format.\n");
		(void)printf("iscale=%d\n",iscale);
		fread(&nbytes,4,1,in_file_ptr);
		fseek(in_file_ptr, 40, SEEK_SET);
		printf("number of bytes %d\n",nbytes);
		/*exit(0);*/
   }
	else if ((int) numbytes==4)
	{
	   /* read first header line */
		fseek(in_file_ptr, 4, SEEK_SET);
		for(i=0;i<2;i++)
	   {
		   fread(&bnd[i], sizeof(float), 1, in_file_ptr);
			printf("bnd %f\n",bnd[i]);
	   }
		for(i=0;i<2;i++)
	   {
		   fread(&det[i], sizeof(int), 1, in_file_ptr);
	   }
		nonxmrg=1;
		
		/* read second header line */
		
		fseek(in_file_ptr, 28, SEEK_SET);
		fread(&iscale,sizeof(int),1,in_file_ptr);
	   (void)printf("Reading cell parameter file format.\n");
		(void)printf("iscale=%d\n",iscale);
		fseek(in_file_ptr, 36, SEEK_SET);
		printf("number of bytes %d\n",nbytes);
		/*exit(0);*/
   }
	else if ((int) numbytes == (ncols*2))
	/* the second record of the files was nonexistent in pre-June 1997 files.*/
	{   
	   /* read first header line */
		fseek(in_file_ptr, 4, SEEK_SET);
	   for(i=0;i<4;i++)
	   {
		   fread(&rfchd[i], sizeof(int), 1, in_file_ptr);
	   }
		(void)printf("Reading pre-1997 format.\n");
		fseek(in_file_ptr,24, SEEK_SET);
		iscale=100;
	}	
	else
	{
	  (void)printf("numbytes %d\n",numbytes);
	   (void)printf("Header file is in a nonstandard format. Data NOT READ!\n");
		exit(1);
	}
	/* victor's header allows xorigin and yorigin to be */
	/* non-integers while xmrg does not                 */
   if (nonxmrg)
	   {
		XORf = bnd[0];
		YORf = bnd[1];
	   XOR = rint(XORf);
		YOR = rint(YORf);
		MAXX = det[0];
		MAXY = det[1];
      }	   
	else	
	   {
		XOR=rfchd[0];
	   YOR=rfchd[1];
		MAXX=rfchd[2];
		MAXY=rfchd[3];
		}
   
	if (nonxmrg)
	   {
	   sterxll=XORf*HRAPCELLSIZE-401.0*HRAPCELLSIZE;
	   steryll=YORf*HRAPCELLSIZE-1601.0*HRAPCELLSIZE;
		}
   else
	   {
		sterxll=XOR*HRAPCELLSIZE-401.0*HRAPCELLSIZE;
	   steryll=YOR*HRAPCELLSIZE-1601.0*HRAPCELLSIZE;
		}
	nrows = MAXY;
	ncols = MAXX;
	
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
	fseek(in_file_ptr, 4, SEEK_CUR);
		for(j=0;j<ncols;j++)
		{
		matrix[i][j] = (float) onerow[j];

		} /* close j */
	} /* close i  */

   reccount=1;
	fprintf(out_file_ptr,"id,val(in)\n");
	for(i=nrows-1; i>-1; i--)
	{
		for(j=0; j<ncols; j++)
		{
			outval=matrix[i][j];
			if (matrix[i][j] < 0)
			{
				outval=-1.0;
			} 
			else
			{
			   outval = outval/2540.0;
				/*  convert from hundredths of mm to inches*/
		   }
         /*fwrite(&outval,4,1,out_file_ptr);*/
			fprintf(out_file_ptr,"%ld,%f\n",reccount,outval);
			reccount = reccount+1;
		}
			/*fprintf(out_file_ptr,"\n");*/
	}

/*free allocated memory*/
free(onerow);
for (i=0;i<nrows;i++) 
{ 
free(matrix[i]); 
}
free(matrix);
fclose(in_file_ptr);
fclose(out_file_ptr);

}  /**  END OF MAIN  **/
