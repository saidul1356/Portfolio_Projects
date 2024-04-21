/**/
/*
Cleaning data in SQL queries
*/
SELECT * FROM NashvilleHousing;
-----------------------------------------------------------------------------------------------
/*
Standarzing date format. Converting from Date & Timestamp to Date only
	↓ ↓ ↓

	SELECT CONVERT(Date, SaleDate) 
	  FROM NashvilleHousing
*/
-----------------------------------------------------------------------------------------------

/*
Updating SaleDate column's date format.
	↓ ↓ ↓
*/

ALTER TABLE NashvilleHousing
ADD SaleDateConverted DATE;

UPDATE NashvilleHousing
	   SET SaleDateConverted = CONVERT(Date, SaleDate)

SELECT SaleDate, SaleDateConverted
  FROM NashvilleHousing;
-----------------------------------------------------------------------------------------------
/*
Populating PropertyAddress for NULL values. 
If there is a NULL, we look at same ParcelID and populate the NULL with address with same ParcelID.
*/

SELECT Addr.ParcelID, Addr.PropertyAddress , NoAddr.ParcelID, NoAddr.PropertyAddress, 
	ISNULL(Addr.PropertyAddress, NoAddr.PropertyAddress) 

FROM NashvilleHousing Addr
JOIN NashvilleHousing NoAddr
  ON Addr.ParcelID = NoAddr.ParcelID
 AND Addr.[UniqueID ] != NoAddr.[UniqueID ]
WHERE Addr.PropertyAddress IS NULL
ORDER BY Addr.ParcelID;

UPDATE Addr
SET Addr.PropertyAddress = ISNULL(Addr.PropertyAddress, NoAddr.PropertyAddress)

FROM NashvilleHousing Addr
JOIN NashvilleHousing NoAddr
  ON Addr.ParcelID = NoAddr.ParcelID
 AND Addr.[UniqueID ] != NoAddr.[UniqueID ]
WHERE Addr.PropertyAddress IS NULL;

-----------------------------------------------------------------------------------------------
/*
Splitting the PropertyAddress column into 2 separate columns: Address, City.
*/
--First seeing different parts in 2 columns and then adding the columns into the table.
--↓ ↓ ↓

SELECT
	SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1 ) as PropAddr, 
	SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) + 1 , LEN(PropertyAddress)) as PropCity

From NashvilleHousing
-----------------------------------------------------------------------------------------------
--Now, adding columns to the table for address and city by splitting PropertyAddress
--↓ ↓ ↓

ALTER TABLE NashvilleHousing
ADD PropAddr NVARCHAR(255),
    PropCity NVARCHAR(255);

--Update the newly added columns with data from the existing column
--↓ ↓ ↓

UPDATE NashvilleHousing
SET PropAddr = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) - 1),
    PropCity = SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) + 2, LEN(PropertyAddress))
WHERE PropertyAddress IS NOT NULL;

-- Explanation:
-- 1. The first ALTER TABLE statement adds two new columns, PropAddr and PropCity, to the NashvilleHousing table.
-- 2. In the UPDATE statement, PropAddr is updated with the substring of PropertyAddress from the beginning up to the comma (`,`), and PropCity is updated with the substring from after the comma to the end of the string.
-- 3. Note that in the substring function for PropCity, I added 2 to the starting index to skip the comma and the following space.
-- 4. The WHERE clause ensures that the update only occurs where the PropertyAddress is not NULL, preventing any potential issues with null values.
SELECT * FROM NashvilleHousing
-----------------------------------------------------------------------------------------------
/*
Splitting OwnerAddress into StreetAddress, City, and State
*/

Select OwnerAddress
From PortfolioProject.dbo.NashvilleHousing

--Explanation:
--When using PARSENAME to split strings, 
--it starts from the rightmost part of the string and works its way to the left, 
--splitting the string based on the specified delimiter.
--Therefore, if you're using PARSENAME to split a string into parts, 
--you have to provide the arguments in reverse order to achieve the desired outcome. 
--That's why in this query, I've used 3,2,1 instead of 1,2,3

Select
PARSENAME(REPLACE(OwnerAddress, ',', '.') , 3) 
,PARSENAME(REPLACE(OwnerAddress, ',', '.') , 2)
,PARSENAME(REPLACE(OwnerAddress, ',', '.') , 1) 
From NashvilleHousing

--Explanation:
--Convert commas to dots before applying PARSENAME for splitting.
--Because PARSENAME function in SQL Server is designed to work specifically with object names
--which are typically dot-separated (e.g., schema.object)

-----------------------------------------------------------------------------------------------

-- Adding 3 new columns to the table for owner address after spltting OwnerAddress
ALTER TABLE NashvilleHousing
ADD SplitAddr NVARCHAR(255),
    SplitCity NVARCHAR(255),
    SplitState NVARCHAR(255);

-- Update 3 new columns to the table for owner address after spltting OwnerAddress

UPDATE NashvilleHousing
SET SplitAddr = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3),
    SplitCity = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2),
    SplitState = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1);

-- Finally checking the new table with the added columns
SELECT * FROM NashvilleHousing;



SELECT SoldAsVacant,
  CASE When SoldAsVacant = 'Y' THEN 'Yes'
       When SoldAsVacant = 'N' THEN 'No'
       ELSE SoldAsVacant
       END
From NashvilleHousing;

--Explanation:
--Selecting SoldAsVacant column from the NashvilleHousing table 
--and applied a CASE statement to it. 
--It checks each value of SoldAsVacant 
--and replaces 'Y' with 'Yes', 'N' with 'No', 
--and leaves other values unchanged.

Update NashvilleHousing
SET SoldAsVacant = CASE When SoldAsVacant = 'Y' THEN 'Yes'
						When SoldAsVacant = 'N' THEN 'No'
						ELSE SoldAsVacant
						END;


--Explanation:
--It updates the SoldAsVacant column in the NashvilleHousing table. 
--It applies the same CASE statement logic 
--as in the SELECT statement to update each value accordingly.

/*

<<Removing duplicates>> from a dataset using the ROW_NUMBER() function 
with a Common Table Expression (CTE) and the PARTITION BY clause.

*/
--Firstly, create CTE (Common Table Expression):
--This CTE computes row numbers (row_num) for each row 
--in the NashvilleHousing partitioned by certain columns 
--(ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference) 
--and ordered by UniqueID.

WITH RowNumCTE AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ParcelID,
                         PropAddr,
                         SalePrice,
                         SaleDateConverted,
                         LegalReference
            ORDER BY UniqueID
        ) AS row_num
    FROM NashvilleHousing
)

--Main Query:
Select *
From RowNumCTE
Where row_num > 1
Order by PropAddr

-- Main Query selects all columns from the CTE where row_num is equal to 1. 
-- Rows with row_num equal to 1 represent the first occurrence of each unique combination 
-- of values in the specified partitioning columns, effectively removing duplicates.

-- By using the ROW_NUMBER() function along with a CTE and filtering for rows where 
-- row_num = 1, I ensure that only one row for each unique combination of values in 
-- the specified columns is retained, effectively removing duplicates.


DELETE 
From RowNumCTE
Where row_num > 1

--After selecting the duplicate rows by using main query, 
--we only need to remove the SELECT * 
--and ORDER BY statements to write our DELETE statement.
--Then highlight the CTE and DELETE statement together to delete duplicates
--After deleting, we can highlight the CTE and Main query (Select), 
--to see if the duplicates are gone.
-----------------------------------------------------------------------------------------------
/* 
Deleting Unused Columns 
*/

ALTER TABLE NashvilleHousing
DROP COLUMN OwnerAddress, 
			TaxDistrict, 
			PropertyAddress,	
			SaleDate;


-- Master Tip
-- Begin by running a SELECT query with the same conditions as 
-- the update or delete statement to preview the affected rows. 
-- This allows you to verify that the correct rows are targeted before making changes.
