<?php
class IlldbBrowser{
	const BASE_COMMAND='python illdb_browser.py';
	private $connectString;
	public function connect($profile){
		//TODO Add mutil profiles support
		require_once 'defines/Illdb.conf.php';
		$confKey=strtoupper("ILLDB_" . $profile);						
		if (!defined("{$confKey}_HOST") ||
			!defined("{$confKey}_PORT") ||
			!defined("{$confKey}_USER") ||
			!defined("{$confKey}_PWD")){
				throw new Exception("Unknown illdb server $profile");				
			}
		$this->connectString=sprintf('--host %s --port %s -u %s -p %s ',
			constant("{$confKey}_HOST"),
			constant("{$confKey}_PORT"),
			constant("{$confKey}_USER"),
			constant("{$confKey}_PWD")
			);
	}

	public function getByKey($bucket,$key){
		$command=self::BASE_COMMAND . " --bucket={$bucket} {$this->connectString} get '$key' ";		
		return $this->run($command);
		
	}

	public function setKeyValue($bucket,$key,$value){
		$command= self::BASE_COMMAND . " --bucket={$bucket} {$this->connectString} set '$key' '$value' ";
		echo($command);
		return $this->run($command);
	}

	protected function run($command) {
		echo $command;
	  $descriptionspec=array(
			0=>['pipe','r'],
			1=>['pipe','w'],
			2=>['pipe','w']
			);
		$handle=proc_open($command,$descriptionspec,$pipes);
		$response=stream_get_line($pipes[1],1024);
		$error=stream_get_line($pipes[2],1024);
		fclose($pipes[0]);
		fclose($pipes[1]);
		fclose($pipes[2]);
		proc_close($handle);
		return array($response,$error);		
	}
}

?>