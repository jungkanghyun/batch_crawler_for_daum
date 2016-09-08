package com.asuscomm.jkh120.code01.batch.domain;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Entity
public class Game {

	@Id
//	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long gameId;
//	private Long id;
	
	private int awayScore;
	private String awaySunbalFirstKname;
	private String awaySunbalFullKname;
	private Long awaySunbalId;
	private String awaySunbalLastKname;
	private String awayTeamFullName;
	private Long awayTeamId;
	private String awayTeamName;
	private String ballparkFullName;
	private Long ballparkId;
	private String ballparkName;
	private String broadId;
	private String broadName;
	private String categoryCode;
	private String confirmId;
	private String cpAwayTeamId;
	private String cpGameId;
	private String cpHomeTeamId;
	private String crud;
	private String dheader;
	private String gameDate;
	private String gameDay;
	private String gameMonth;
	private String gameTime;
	private String gameWeek;
	private String gameWeekday;
	private String gameWeekDate;
	private String grandCategoryCode;
	private int homeScore;
	private String homeSunbalFirstKname;
	private String homeSunbalFullKname;
	private Long homeSunbalId;
	private String homeSunbalLastKname;
	private String homeTeamFullName;
	private Long homeTeamId;
	private String homeTeamName;
	private String seasonId;
	private Long seriesId;
	private String seriesName;
	private String status;
	private String stReason;
	private String weather;
	
	public Game() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	
}
