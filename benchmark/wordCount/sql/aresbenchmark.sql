/*
Navicat MySQL Data Transfer

Source Server         : 华科ubuntu2
Source Server Version : 50721
Source Host           : ubuntu2:3306
Source Database       : aresbenchmark

Target Server Type    : MYSQL
Target Server Version : 50721
File Encoding         : 65001

Date: 2018-04-03 22:03:56
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for t_aresspoutcount
-- ----------------------------
DROP TABLE IF EXISTS `t_aresspoutcount`;
CREATE TABLE `t_aresspoutcount` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tuplecount` bigint(20) NOT NULL,
  `taskid` int(11) NOT NULL,
  `time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_aresspoutlatency
-- ----------------------------
DROP TABLE IF EXISTS `t_aresspoutlatency`;
CREATE TABLE `t_aresspoutlatency` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `latencytime` double NOT NULL,
  `taskid` int(11) NOT NULL,
  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_areswordcount
-- ----------------------------
DROP TABLE IF EXISTS `t_areswordcount`;
CREATE TABLE `t_areswordcount` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `word` varchar(20) NOT NULL,
  `count` bigint(20) NOT NULL,
  `taskid` int(11) NOT NULL,
  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_defaultspoutcount
-- ----------------------------
DROP TABLE IF EXISTS `t_defaultspoutcount`;
CREATE TABLE `t_defaultspoutcount` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tuplecount` bigint(20) NOT NULL,
  `taskid` int(11) NOT NULL,
  `time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6248 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_defaultspoutlatency
-- ----------------------------
DROP TABLE IF EXISTS `t_defaultspoutlatency`;
CREATE TABLE `t_defaultspoutlatency` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `latencytime` double NOT NULL,
  `taskid` int(11) NOT NULL,
  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
