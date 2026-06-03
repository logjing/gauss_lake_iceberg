/*
 * opengauss_compat.h - openGauss 兼容层
 */

#ifndef OPENGAUSS_COMPAT_H
#define OPENGAUSS_COMPAT_H

#include "postgres.h"

/* SPI 接口适配 - openGauss 需要额外参数 */
#define SPI_CONNECT_COMPAT() SPI_connect(DestSPI, NULL, NULL)
#define SPI_EXECUTE_COMPAT(q, r, t) SPI_execute(q, r, t, false, NULL)
#define SPI_FINISH_COMPAT() SPI_finish()

/* 系统表访问 */
#define TABLE_OPEN_COMPAT(relid, lockmode) heap_open(relid, lockmode)
#define TABLE_CLOSE_COMPAT(rel, lockmode) heap_close(rel, lockmode)

#endif /* OPENGAUSS_COMPAT_H */