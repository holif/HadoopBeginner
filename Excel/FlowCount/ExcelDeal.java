package xyz.baal.mr;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;

public class ExcelDeal {

	public static String[] readExcel(InputStream is) throws IOException {

		@SuppressWarnings("resource")
		HSSFWorkbook hssfWorkbook = new HSSFWorkbook(is);

		List<String> list = new ArrayList<String>();

		for (int numSheet = 0; numSheet < hssfWorkbook.getNumberOfSheets(); numSheet++) {
			HSSFSheet hssfSheet = hssfWorkbook.getSheetAt(numSheet);
			if (hssfSheet == null) {
				continue;
			}
			for (int rowNum = 1; rowNum <= hssfSheet.getLastRowNum(); rowNum++) {
				String templine = new String();
				HSSFRow hssfRow = hssfSheet.getRow(rowNum);
				if (hssfRow == null) {
					continue;
				}
				//遍历每一行数据
				for (Cell cell : hssfRow) {
					templine += getValue(cell)+"\t";
				}
				list.add(templine);
			}
		}
		return list.toArray(new String[0]);
	}

	//根据数据类型获取每个表格中的数据
	private static String getValue(Cell cell) {
		if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
			return String.valueOf(cell.getBooleanCellValue());
		} else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
			if (HSSFDateUtil.isCellDateFormatted(cell)) {
				SimpleDateFormat sdf = null;
				sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date = cell.getDateCellValue();
				return sdf.format(date);
			}
			return String.valueOf(cell.getNumericCellValue());
		} else {
			return String.valueOf(cell.getStringCellValue());
		}
	}
}
