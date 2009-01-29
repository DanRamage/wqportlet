/***************************************************************************
 *   Copyright (C) 2009 by dramage,,,   *
 *   dramage@TestBed   *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 *   This program is distributed in the hope that it will be useful,       *
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
 *   GNU General Public License for more details.                          *
 *                                                                         *
 *   You should have received a copy of the GNU General Public License     *
 *   along with this program; if not, write to the                         *
 *   Free Software Foundation, Inc.,                                       *
 *   59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.             *
 ***************************************************************************/


#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

typedef struct
{
  double x;
  double y;
} HRAP;


HRAP HrapToLatLong(double,double);


/* This utility program was written by Mark J. Fenbers at OHRFC in the late
Winter of 1997. It is one that reads the XMRG format (as it is conventionally
called), first utilized by Stage 3 software from OH, and determines the
lat/lon coordinates for each grid point and writes these out along with the
grid point's value. In other words, it is an XMRG to ASCII reformatter for
use by other programs. Output grid values are converted from 100ths of mm to
hundredths of inches. */


int main(int argc,char **argv)
{
  FILE *fpi, *outFile;
  int fourbytes,hrapi,hrapj,numi,numj,i,j;
  char userid[11],dttm[21],process[9],leftover[120];
  unsigned short int zval;
  HRAP ll;

  memset( userid, 0, sizeof(userid));
  memset( dttm, 0, sizeof(dttm));
  memset( process, 0, sizeof(process));
  memset( leftover, 0, sizeof(leftover));
  /* If no command-line arguments are supplied, use "standard input,"
  otherwise, attempt to open file given as command-line arg. Output is
  sent
  to "standard output." */
  if(argc < 3)
    fpi = stdin;
  else
    if((fpi = fopen(argv[1],"rb")) == NULL)
      perror(argv[1]),exit(-1);
    //sprintf( strOutFilename, "%s.log", strFilename );
    if(( outFile = fopen(argv[2],"wa")) == NULL )
    {
	char Msg[256];
        sprintf( "Unable to open output file: %s", argv[2] );
	perror( Msg );
	exit(-1);
     } 
  /* The XMRG file format is a FORTRAN "free-format." FORTRAN (using
  free
  format) encapsulates each data record with a four-byte integer giving
  the
  number of bytes in the data record. These integers before and after
  each
  data record can be ignored in C programs (after being read in).
  "fourbytes" is a dummy variable to read in this wrapper from the
  record. */

  /* Read in starting HRAP coordinate and number of rows/cols */
  fread(&fourbytes,1,4,fpi);
  fread(&hrapi,1,4,fpi);
  fread(&hrapj,1,4,fpi);
  fread(&numi,1,4,fpi);
  fread(&numj,1,4,fpi);
  if(fourbytes - 16 > 0)
    fread(leftover,1,fourbytes - 16, fpi);
  fread(&fourbytes,1,4,fpi);
  //fprintf(stderr,"hrap-i,j = %d,%d nx,ny =  %d,%d\n",hrapi,hrapj,numi,numj);
  fprintf(outFile,"hrap-i,j = %d,%d nx,ny =  %d,%d\n",hrapi,hrapj,numi,numj);

  /* Read in creation information (user, date/time, generation process) */
  fread(&fourbytes,1,4,fpi);
  fread(userid,1,10,fpi);
  fread(dttm,1,20,fpi);
  fread(process,1,8,fpi);
  if(fourbytes - 42 > 0)
    fread(leftover,1,fourbytes - 42, fpi);
  fread(&fourbytes,1,4,fpi);
  //fprintf(stderr,"userid = \"%s\"\ndate/time = \"%s\"\nprocess =  \"%s\"\n",userid,dttm,process);
  fprintf(outFile,"userid = \"%s\" date/time = \"%s\"process =  \"%s\"\n",userid,dttm,process);

  /* Loop to read/process/write each grid point */
  for(j = 0; j < numj; j++)
  {
    fread(&fourbytes,1,4,fpi);
    for(i = 0; i < numi; i++)
    {
      fread(&zval,1,2,fpi);
      //if(zval & 0x8000)
      //  continue;
      ll = HrapToLatLong((double)(hrapi + i),(double)(hrapj + j));
      //fprintf(stdout,"%9.4f %7.4f      %.2f\n",-ll.x,ll.y,(double)zval / 2540.0);
      fprintf(outFile,"%d,%d,%9.4f,%7.4f,%.2f\n",(hrapi + i), (hrapj + j), -ll.x,ll.y,(double)zval / 2540.0);
      /* fprintf(stdout,"%9.4f %7.4f
      %x\n",-ll.x,ll.y,zval); */
    }
    fread(&fourbytes,1,4,fpi);
  }

  fclose(fpi);

  return(0);
}


/**************************************************************************/
/* HrapToLatLong: converts from HRAP coordinates to latitude-longitude */
/***************************************************************************

Function type:
HRAP structure

Called by function:
main

Functions called:
none

Global variables:
HrapToLatLong - function
LatLongToHrap - function

Local variables:
tlat - float; standard latitude in radians
earthr - float; earth's radius (km)
xmesh - float; mesh length at 60 deg North
raddeg - float; conversion from radians to degrees
stlon - float; standard longitude
ll - HRAP structure; latitude/longitude

******************************************** BEGIN HrapToLatLong ***********/

HRAP HrapToLatLong(double hrap_col, double hrap_row)
{
double x, y, rr, gi, ang;
double tlat, earthr, xmesh, raddeg, stlon;
HRAP ll;

earthr = 6371.2;
stlon = 105.0;
raddeg = 57.29577951;
xmesh = 4.7625;
tlat = 60.0 / raddeg;

x = hrap_col - 401.0;
y = hrap_row - 1601.0;
rr = x * x + y * y;
gi = ((earthr * (1.0 + sin(tlat))) / xmesh);
gi *= gi;
ll.y = asin((gi - rr) / (gi + rr)) * raddeg;

ang = atan2(y,x) * raddeg;

if(ang < 0.0)
ang += 360.0;
ll.x = 270.0 + stlon - ang;

if(ll.x < 0.0)
ll.x += 360.0;
else if(ll.x > 360.0)
ll.x -= 360.0;

return ll;
}

/********************************************* END HrapToLatLong ***********/
